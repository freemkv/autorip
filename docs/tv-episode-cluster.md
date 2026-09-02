# TV episode-length clustering

`episode_cluster` in `src/ripper/tv.rs` finds the indices whose duration sits
in the modal episode-length cluster.

It uses the median of the above-minimum candidates as the cluster centre and
keeps everything within a generous tolerance (the larger of 5 min and 25%).
A "play all" title (~N times an episode) lands far above the median and is
dropped; short extras land far below and are dropped. With 0-1 candidates
there is no cluster to speak of, so the candidates pass through unchanged.
