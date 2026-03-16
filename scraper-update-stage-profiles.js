const {
  scrapeFirstCyclingRace,
  fetchFirstCyclingWorldTourRaces,
  resolveStageProfileImageUrls,
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

const buildStageSummary = (race, stageNumber) => (
  `${race.slug.replace(/-/g, ' ')} - Stage ${stageNumber}. ` +
  'Route profile update for upcoming stage.'
);

const normalizeTtLabel = (value) => {
  const upper = String(value || '').toUpperCase();
  if (upper === 'TTT' || upper === 'ITT') return upper;
  return null;
};

const extractTtLabelFromText = (value) => {
  const text = String(value || '').toLowerCase();
  if (!text) return null;
  if (/\bttt\b|team\s*time\s*trial|ploegen\s*tijdrit/.test(text)) return 'TTT';
  if (/\bitt\b|individual\s*time\s*trial|prologue|\btijdrit\b/.test(text)) return 'ITT';
  return null;
};

const buildStageProfileTitle = (stageNumber, ttLabel) => (
  ttLabel ? `Stage ${stageNumber} (${ttLabel})` : `Stage ${stageNumber}`
);

const buildStageProfileSummary = (race, stageNumber, ttLabel) => {
  if (ttLabel === 'TTT') {
    return `${race.slug.replace(/-/g, ' ')} - Stage ${stageNumber}. Team time trial stage.`;
  }
  if (ttLabel === 'ITT') {
    return `${race.slug.replace(/-/g, ' ')} - Stage ${stageNumber}. Individual time trial stage.`;
  }
  return buildStageSummary(race, stageNumber);
};

async function updateStageProfilesForRace(race, raceRecord, updateAllRaces = false) {
  console.log(`\n🗺️  Updating stage profiles for ${raceRecord.name}...`);

  const { data: stages, error: stagesError } = await supabase
    .from('stages')
    .select('id, stage_number, start_date, is_rest_day')
    .eq('race_id', raceRecord.id)
    .order('stage_number', { ascending: true });

  if (stagesError) {
    console.log(`  ⚠️  Error loading stages: ${stagesError.message}`);
    return;
  }

  // Filter to upcoming stages only if updateAllRaces is false
  const stagesToUpdate = updateAllRaces 
    ? (stages || []) 
    : (stages || []).filter(stage => !isPastDate(stage.start_date));
    
  if (stagesToUpdate.length === 0) {
    console.log('  ℹ️  No stages found in database');
    return;
  }

  console.log(`  📊 Updating ${stagesToUpdate.length} stage(s)${updateAllRaces ? ' (including past)' : ' (upcoming only)'}`);

  // Exclude rest days from profile updates
  const stagesWithoutRestDays = stagesToUpdate.filter(s => !s.is_rest_day);
  if (stagesWithoutRestDays.length < stagesToUpdate.length) {
    const restDayCount = stagesToUpdate.length - stagesWithoutRestDays.length;
    console.log(`  🚫 Excluding ${restDayCount} rest day(s) from profile updates`);
  }
  if (stagesWithoutRestDays.length === 0) {
    console.log('  ℹ️  No non-rest stages to update');
    return;
  }

  const raceData = await scrapeFirstCyclingRace(race);
  const ttLabelByStageNumber = new Map();
  (raceData && Array.isArray(raceData.stages) ? raceData.stages : []).forEach((stage) => {
    const stageNumber = Number(stage.stage_number);
    if (!Number.isFinite(stageNumber)) return;
    const label = normalizeTtLabel(stage.tt_label) || extractTtLabelFromText(stage.name || stage.description || '');
    if (label) {
      ttLabelByStageNumber.set(stageNumber, label);
    }
  });
  const iconUrlByStageNumber = new Map();
  (raceData && Array.isArray(raceData.stages) ? raceData.stages : []).forEach((stage) => {
    const stageNumber = Number(stage.stage_number);
    if (!Number.isFinite(stageNumber)) return;
    const iconUrl = String(stage.stage_icon_url || '').trim();
    if (iconUrl) {
      iconUrlByStageNumber.set(stageNumber, iconUrl);
    }
  });

  const stageIds = stagesWithoutRestDays.map((stage) => stage.id).filter(Boolean);
  const stageProfilesByStageId = new Map();
  if (stageIds.length > 0) {
    const { data: existingProfiles } = await supabase
      .from('stage_profiles')
      .select('stage_id,title,summary')
      .in('stage_id', stageIds);

    (existingProfiles || []).forEach((profile) => {
      stageProfilesByStageId.set(profile.stage_id, profile);
    });
  }

  for (const stage of stagesWithoutRestDays) {
    await sleep(150);

    const existingProfile = stageProfilesByStageId.get(stage.id) || null;
    const fallbackLabel = extractTtLabelFromText(`${existingProfile ? existingProfile.title : ''} ${existingProfile ? existingProfile.summary : ''}`);
    const ttLabel = ttLabelByStageNumber.get(Number(stage.stage_number)) || fallbackLabel || null;

    const { jpgUrl, pngUrl } = await resolveStageProfileImageUrls(race, stage.stage_number, stages.length);
    const stageProfile = {
      stage_id: stage.id,
      title: buildStageProfileTitle(stage.stage_number, ttLabel),
      summary: buildStageProfileSummary(race, stage.stage_number, ttLabel),
      stage_icon_url: iconUrlByStageNumber.get(Number(stage.stage_number)) || null,
      profile_image_url_jpg: jpgUrl,
      profile_image_url_png: pngUrl,
      scraped_at: new Date().toISOString(),
    };

    const { error: upsertError } = await supabase
      .from('stage_profiles')
      .upsert([stageProfile], { onConflict: 'stage_id' });

    if (upsertError) {
      console.log(`  ⚠️  Error updating stage ${stage.stage_number}: ${upsertError.message}`);
    } else {
      console.log(`  ✅ Updated stage ${stage.stage_number}`);
    }
  }
}

async function main() {
  console.log('🚀 Starting stage-profile update for upcoming races/stages...');
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

    await updateStageProfilesForRace(race, existingRace, updateAllRaces);

    if (i < races.length - 1) {
      const delay = 2000 + Math.random() * 2000;
      console.log(`\n⏳ Waiting ${(delay / 1000).toFixed(1)}s...`);
      await sleep(delay);
    }
  }

  console.log('\n✅ Stage-profile update complete!');
}

if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
