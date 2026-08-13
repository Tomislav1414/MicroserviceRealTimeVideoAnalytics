// Must match sessionapi's DETECTOR_TYPES allowlist (sessionapi/config.go)
// and risingwave's DETECTOR_TYPES (risingwave/init.sh) — all three need to
// agree on which detector types exist.
export const DETECTORS = ["human", "car"];
