const {
  fetchFirstCyclingWorldTourRaces,
  scrapeFirstCyclingStartlist,
  scrapePcsRiderStatusWithStage,
  supabase,
} = require('./scraper-cycling-archives');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const getEnvValue = (key, fallback) => {
  const value = process.env[key];
  return value && value.trim() ? value.trim() : fallback;
};

const getTodayUtcDateOnly = () => {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
};

const parseDateOnlyUtc = (dateStr) => {
  if (!dateStr) {
    return null;
  }

  const match = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }

  return new Date(Date.UTC(year, month - 1, day));
};

const isPastDate = (dateStr) => {
  const date = parseDateOnlyUtc(dateStr);
  if (!date) {
    return false;
  }

  return date < getTodayUtcDateOnly();
};

const WORLD_TOUR_YEAR = Number(getEnvValue('WT_YEAR', '2026'));
const WORLD_TOUR_URL = getEnvValue(
  'FIRSTCYCLING_WT_URL',
  `https://firstcycling.com/race.php?y=${WORLD_TOUR_YEAR}&t=1`
);

function normalizeStageTime(value) {
  if (!value) return '';
  if (value.includes('T')) return value.split('T')[1].slice(0, 5);
  if (value.includes(' ')) return value.split(' ')[1].slice(0, 5);
  return value.slice(0, 5);
}

function getStageStartDateTime(stage) {
  if (!stage || !stage.start_date) return null;
  const timeValue = normalizeStageTime(stage.start_time || '');
  const timePart = timeValue ? `${timeValue}:00` : '00:00:00';
  return new Date(`${stage.start_date}T${timePart}`);
}

function isDnsDnfStatus(status) {
  const s = String(status || '').toUpperCase();
  return s.includes('DNF') || s.includes('DNS');
}

function normalizeRiderLookupName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

async function hasRiderDnfStageColumns() {
  const { error } = await supabase
    .from('riders')
    .select('id,dnf_stage_number,dnf_detected_at')
    .limit(1);

  return !error;
}

async function getLatestDnfStageNumberForRace(raceId, detectedAtIso) {
  const { data: stages, error } = await supabase
    .from('stages')
    .select('stage_number,start_date,is_rest_day')
    .eq('race_id', raceId)
    .order('stage_number', { ascending: true });

  if (error || !stages || stages.length === 0) {
    return null;
  }

  const activeStages = stages
    .filter((stage) => !stage.is_rest_day)
    .map((stage) => ({
      stage_number: Number(stage.stage_number),
      start_date: String(stage.start_date || '').trim()
    }))
    .filter((stage) => Number.isFinite(stage.stage_number))
    .sort((a, b) => a.stage_number - b.stage_number);

  if (activeStages.length === 0) return null;

  const detectedDate = new Date(detectedAtIso || Date.now());
  const yyyy = detectedDate.getFullYear();
  const mm = String(detectedDate.getMonth() + 1).padStart(2, '0');
  const dd = String(detectedDate.getDate()).padStart(2, '0');
  const detectedYmd = `${yyyy}-${mm}-${dd}`;

  // Day-after workflow: map to the stage before the first stage scheduled on/after detected date.
  const boundaryIndex = activeStages.findIndex((stage) => stage.start_date && stage.start_date >= detectedYmd);
  if (boundaryIndex > 0) {
    return activeStages[boundaryIndex - 1].stage_number;
  }
  if (boundaryIndex === 0) {
    return activeStages[0].stage_number;
  }

  return activeStages[activeStages.length - 1].stage_number;
}

async function updateRidersForRace(race, raceRecord) {
  console.log(`\n🚴 Updating riders for ${raceRecord.name}...`);

  const riders = await scrapeFirstCyclingStartlist(race);
  if (!riders || riders.length === 0) {
    console.log('  ⚠️  No riders scraped; skipping update to avoid wiping existing data');
    return;
  }

  const pcsRiderStatusByName = await scrapePcsRiderStatusWithStage(race);
  console.log(`  Found ${pcsRiderStatusByName.size} riders with PCS DNF/DNS status`);

  const scrapeDetectedAt = new Date().toISOString();
  const riderDnfStageColumnsSupported = await hasRiderDnfStageColumns();
  const latestDnfStageNumber = riderDnfStageColumnsSupported
    ? await getLatestDnfStageNumberForRace(raceRecord.id, scrapeDetectedAt)
    : null;

  // Look up rider URLs
  console.log(`  🔗 Processing ${riders.length} riders with URLs...`);
  const ridersWithUrls = [];
  for (let i = 0; i < riders.length; i++) {
    const riderObj = riders[i];
    const name = typeof riderObj === 'string' ? riderObj : riderObj.name;
    const team = typeof riderObj === 'string' ? '' : (riderObj.team || '');
    const riderUrl = typeof riderObj === 'object' ? riderObj.rider_url : null;
    
    let status = null;
    const normalizedName = normalizeRiderLookupName(name);
    const pcsEntry = pcsRiderStatusByName.get(normalizedName);
    if (pcsEntry && pcsEntry.status) {
      status = pcsEntry.status;
    }

    ridersWithUrls.push({
      name,
      bib_number: i + 1,
      team,
      status,
      rider_url: riderUrl,
    });
  }

  const scrapedRiders = ridersWithUrls;

  const { data: existingRiders, error: existingRidersError } = await supabase
    .from('riders')
    .select(riderDnfStageColumnsSupported
      ? 'id, name, status, dnf_stage_number, dnf_detected_at'
      : 'id, name, status')
    .eq('race_id', raceRecord.id);

  if (existingRidersError) {
    console.log(`  ⚠️  Error loading existing riders: ${existingRidersError.message}`);
    return;
  }

  const existingByName = new Map(
    (existingRiders || []).map(existingRider => [
      String(existingRider.name || '').trim().toLowerCase(),
      existingRider,
    ])
  );

  const ridersToUpdate = [];
  const ridersToInsert = [];
  const scrapedNames = new Set(scrapedRiders.map(r => String(r.name || '').trim().toLowerCase()));

  for (const rider of scrapedRiders) {
    const riderKey = String(rider.name || '').trim().toLowerCase();
    const existingRider = existingByName.get(riderKey);
    const currentIsDnfDns = isDnsDnfStatus(rider.status);
    const previousIsDnfDns = existingRider ? isDnsDnfStatus(existingRider.status) : false;
    const normalizedName = normalizeRiderLookupName(rider.name);
    const pcsEntry = pcsRiderStatusByName.get(normalizedName);

    const pcsStageNumber = Number(pcsEntry && pcsEntry.stage_number);
    const inferredDnfStageNumber = Number.isFinite(pcsStageNumber) ? pcsStageNumber : latestDnfStageNumber;

    const dnfStageNumber = currentIsDnfDns
      ? (Number.isFinite(Number(existingRider?.dnf_stage_number)) && previousIsDnfDns
        ? Number(existingRider.dnf_stage_number)
        : inferredDnfStageNumber)
      : null;

    const dnfDetectedAt = currentIsDnfDns
      ? ((existingRider?.dnf_detected_at && previousIsDnfDns)
        ? existingRider.dnf_detected_at
        : scrapeDetectedAt)
      : null;

    if (existingRider) {
      const updateRow = {
        id: existingRider.id,
        name: rider.name,
        bib_number: rider.bib_number,
        team: rider.team,
        status: rider.status,
        rider_url: rider.rider_url,
      };

      if (riderDnfStageColumnsSupported) {
        updateRow.dnf_stage_number = dnfStageNumber;
        updateRow.dnf_detected_at = dnfDetectedAt;
      }

      ridersToUpdate.push(updateRow);
    } else {
      const insertRow = {
        race_id: raceRecord.id,
        name: rider.name,
        bib_number: rider.bib_number,
        team: rider.team,
        status: rider.status,
        rider_url: rider.rider_url,
      };

      if (riderDnfStageColumnsSupported) {
        insertRow.dnf_stage_number = dnfStageNumber;
        insertRow.dnf_detected_at = dnfDetectedAt;
      }

      ridersToInsert.push(insertRow);
    }
  }

  // Find riders to delete (in database but not in current startlist)
  const ridersToDelete = [];
  for (const existingRider of (existingRiders || [])) {
    const existingKey = String(existingRider.name || '').trim().toLowerCase();
    if (!scrapedNames.has(existingKey)) {
      ridersToDelete.push(existingRider.id);
    }
  }

  if (ridersToUpdate.length > 0) {
    const { error: updateError } = await supabase
      .from('riders')
      .upsert(ridersToUpdate, { onConflict: 'id' });

    if (updateError) {
      console.log(`  ⚠️  Error updating riders: ${updateError.message}`);
      return;
    }
  }

  if (ridersToInsert.length > 0) {
    const { error: insertError } = await supabase
      .from('riders')
      .insert(ridersToInsert);

    if (insertError) {
      console.log(`  ⚠️  Error inserting riders: ${insertError.message}`);
    }
  }

  // Delete riders that are no longer on the startlist
  if (ridersToDelete.length > 0) {
    const { error: deleteError } = await supabase
      .from('riders')
      .delete()
      .in('id', ridersToDelete);

    if (deleteError) {
      console.log(`  ⚠️  Error deleting riders: ${deleteError.message}`);
    } else {
      console.log(`  🗑️  Deleted ${ridersToDelete.length} riders that are no longer on the startlist`);
    }
  }

  if (ridersToInsert.length === 0 && ridersToDelete.length === 0) {
    console.log(`  ✅ Updated ${ridersToUpdate.length} riders, no changes needed`);
  } else {
    console.log(`  ✅ Updated ${ridersToUpdate.length} riders, inserted ${ridersToInsert.length} new riders`);
  }
}

async function main() {
  console.log('🚀 Starting riders-only update...');
  console.log(`📍 Fetching UWT races for ${WORLD_TOUR_YEAR}...`);

  // Check if we should update all races (including past ones)
  const { data: settingsData, error: settingsError } = await supabase
    .from('settings')
    .select('value')
    .eq('id', 'update_all_races')
    .single();

  const updateAllRaces = settingsData && settingsData.value === true;
  console.log(`📋 Update all races setting: ${updateAllRaces}`);

  const races = await fetchFirstCyclingWorldTourRaces(WORLD_TOUR_YEAR, WORLD_TOUR_URL);
  if (races.length === 0) {
    console.log('❌ No UWT races found. Check FIRSTCYCLING_COOKIE or calendar URL.');
    process.exit(1);
  }

  const { data: existingRaces, error: existingRacesError } = await supabase
    .from('races')
    .select('id, slug, name');

  if (existingRacesError) {
    console.log(`❌ Failed to load existing races: ${existingRacesError.message}`);
    process.exit(1);
  }

  const existingRaceMap = new Map((existingRaces || []).map(race => [race.slug, race]));

  for (let i = 0; i < races.length; i++) {
    const race = races[i];
    const existingRace = existingRaceMap.get(race.slug);

    if (!existingRace) {
      console.log(`\n⏭️  Skipping ${race.name} (not in database)`);
      continue;
    }

    // Only skip completed races if updateAllRaces is false
    if (!updateAllRaces) {
      // Check if race is completely finished (for stage races, check if all stages are done)
      const { data: stages } = await supabase
        .from('stages')
        .select('start_date')
        .eq('race_id', existingRace.id)
        .order('start_date', { ascending: false });

      let isCompletelyFinished = false;
      
      if (stages && stages.length > 0) {
        // For stage races: check if the last stage is in the past
        const lastStageDate = stages[0].start_date;
        isCompletelyFinished = isPastDate(lastStageDate);
      } else {
        // For one-day races: check if race start date is in the past
        isCompletelyFinished = isPastDate(race.start_date);
      }

      if (isCompletelyFinished) {
        console.log(`\n⏭️  Skipping ${race.name} (race finished)`);
        continue;
      }
    }

    console.log(`\n${'='.repeat(50)}`);
    console.log(`[${i + 1}/${races.length}] ${race.name}`);
    console.log(`${'='.repeat(50)}`);

    await updateRidersForRace(race, existingRace);

    if (i < races.length - 1) {
      const delay = 2000 + Math.random() * 2000;
      console.log(`\n⏳ Waiting ${(delay / 1000).toFixed(1)}s...`);
      await sleep(delay);
    }
  }

  console.log('\n✅ Riders-only update complete!');
}

if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
