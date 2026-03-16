-- Persist DNF/DNS detection metadata per rider.
-- Safe to run multiple times.

alter table if exists public.riders
  add column if not exists dnf_stage_number integer;

alter table if exists public.riders
  add column if not exists dnf_detected_at timestamptz;

create index if not exists riders_dnf_stage_number_idx
  on public.riders (dnf_stage_number);

create index if not exists riders_dnf_detected_at_idx
  on public.riders (dnf_detected_at);
