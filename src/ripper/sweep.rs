//! Pass 1 sweep + Pass 2..N retry orchestration.
//!
//! Placeholder for the 0.18 trait migration: today the sweep + retry
//! loop lives inline inside `super::rip_disc` because `Disc::copy` /
//! `Disc::patch` are 0.17.13 APIs that don't yet expose a clean
//! Source/Sink boundary we can lift out without semantic change.
//! Once libfreemkv 0.18 ships its `Pipeline` + `SectorSource` /
//! `SectorSink` traits, the multipass loop currently inside
//! `rip_disc` migrates here.
//!
//! See `freemkv-private/memory/0_18_redesign.md` § "Module layout".
