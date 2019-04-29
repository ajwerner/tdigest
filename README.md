# tdigest

### Status: <span style="color:red"> In Development </span>

This library is a concurrent go implementation of the t-digest data structure
for streaming quantile estimation. The code implements the zero-allocation,
merging algorithm from Ted Dunning's paper ([here][histo.pdf]).

The implementation strives to make concurrent writes cheap. In the common case
a write needs only increment an atomic and write two floats to a buffer.
Occasionlly, when the buffer fills, a caller will have to perform the merge
operation.

[histo.pdf]: https://github.com/tdunning/t-digest/raw/d7427ee41be6a6fd271206f26a0cad42f74f30bf/docs/t-digest-paper/histo.pdf
