//! Mux frame loop — read PES frames, write to the chosen output sink,
//! drive the watchdog, push per-frame UI state.
//!
//! Placeholder for the 0.18 trait migration: today the mux loop lives
//! inline inside `super::rip_disc` because the 0.17.13 `pes::Stream`
//! API is a unified read+write trait, and lifting it out without a
//! Source/Sink split would cross the "no behavior change" line for
//! this prep slice. Once libfreemkv 0.18 splits `pes::Stream` into
//! `FrameSource` + `FrameSink` and ships `Pipeline<I, R>`, the mux
//! loop currently inside `rip_disc` migrates here.
//!
//! See `freemkv-private/memory/0_18_redesign.md` § "Module layout".
