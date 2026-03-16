const { supabase, scrapePcsRiderStatusWithStage } = require('../scraper-cycling-archives');

function normalizeRiderLookupName(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function isDnsDnfStatus(status) {
  const s = String(status || '').toUpperCase();
  return s.includes('DNF') || s.includes('DNS');
}

async function hasRiderDnfStageColumns() {
  const { error } = await supabase
    .from('riders')
    .select('id,dnf_stage_number,dnf_detected_at')
    .limit(1);
  return !error;
}

async function fetchAllRaces() {
  const all = [];
  let offset = 0;
  const pageSize = 500;

  let raceSelect = 'id,name,slug,year,start_date';
  let checkedStartDateSupport = false;

  while (true) {
    const { data, error } = await supabase
      .from('races')
      .select(raceSelect)
      .order('id', { ascending: true })
      .range(offset, offset + pageSize - 1);

    if (error && !checkedStartDateSupport && String(error.message || '').toLowerCase().includes('start_date')) {
      raceSelect = 'id,name,slug,year';
      checkedStartDateSupport = true;
      continue;
    }

    if (error) throw error;
    checkedStartDateSupport = true;
    if (!data || data.length === 0) break;
    all.push(...data);
    if (data.length < pageSize) break;
    offset += pageSize;
  }

  return all;
}

async function fetchAllRidersForRace(raceId) {
  const all = [];
  let offset = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from('riders')
      .select('id,name,status,dnf_stage_number,dnf_detected_at')
      .eq('race_id', raceId)
      .order('id', { ascending: true })
      .range(offset, offset + pageSize - 1);

    if (error) throw error;
    if (!data || data.length === 0) break;
    all.push(...data);
    if (data.length < pageSize) break;
    offset += pageSize;
  }

  return all;
}

async function main() {
  console.log('Starting one-time PCS-only DNF/DNS overwrite for all races...');

  const nowIso = new Date().toISOString();
  const supportsDnfColumns = await hasRiderDnfStageColumns();
  const races = await fetchAllRaces();

  let totalRidersTouched = 0;
  let totalDnfDnsAssigned = 0;

  for (let i = 0; i < races.length; i += 1) {
    const race = races[i];
    const raceYear = Number(race.year) || new Date().getFullYear();
    const raceForPcs = {
      id: race.id,
      name: race.name,
      slug: race.slug,
      year: raceYear,
      start_date: race.start_date || '',
    };

    console.log(`\n[${i + 1}/${races.length}] ${race.name}`);

    const pcsStatusByName = await scrapePcsRiderStatusWithStage(raceForPcs);
    console.log(`  PCS DNF/DNS entries: ${pcsStatusByName.size}`);

    const riders = await fetchAllRidersForRace(race.id);
    if (!riders.length) {
      console.log('  No riders in DB for this race, skipping.');
      continue;
    }

    let raceAssignedCount = 0;
    const updates = riders.map((rider) => {
      const key = normalizeRiderLookupName(rider.name);
      const pcsEntry = pcsStatusByName.get(key);
      const status = pcsEntry && pcsEntry.status ? pcsEntry.status : null;
      const row = {
        id: rider.id,
        race_id: race.id,
        name: rider.name,
        status,
      };

      if (supportsDnfColumns) {
        if (isDnsDnfStatus(status)) {
          const pcsStageNumber = Number(pcsEntry && pcsEntry.stage_number);
          row.dnf_stage_number = Number.isFinite(pcsStageNumber) ? pcsStageNumber : null;
          row.dnf_detected_at = nowIso;
          raceAssignedCount += 1;
        } else {
          row.dnf_stage_number = null;
          row.dnf_detected_at = null;
        }
      }

      return row;
    });

    if (updates.length > 0) {
      const { error } = await supabase
        .from('riders')
        .upsert(updates, { onConflict: 'id' });

      if (error) {
        console.log(`  Error updating riders: ${error.message}`);
        continue;
      }

      totalRidersTouched += updates.length;
      totalDnfDnsAssigned += raceAssignedCount;
      console.log(`  Updated riders: ${updates.length}`);
    }
  }

  console.log('\nPCS overwrite complete.');
  console.log(`Riders updated: ${totalRidersTouched}`);
  console.log(`DNF/DNS assigned from PCS: ${totalDnfDnsAssigned}`);
}

main().catch((err) => {
  console.error('Fatal error:', err.message || err);
  process.exit(1);
});
