# ncbi-gene-pipeline
Load nucleotide and protein sequences for genes, all species, from NCBI.

PROTEIN SEQUENCES

- We load protein sequences for transcript objects.
- Protein sequences are loaded into RGD_SEQUENCES table with seq_type set to 'ncbi_protein'.
- Sometimes NCBI provides new sequence for given transcript.
We load the new protein sequence, and keep the old one with seq_type set to 'old_ncbi_protein'.
Therefore we can have the whole history of changes for transcript reference protein sequence.
