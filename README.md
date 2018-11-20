# ncbi-gene-pipeline
Load rna (nucleotide) and protein sequences for genes, all species, from NCBI.

RNA SEQUENCES

- We load rna sequences for transcript objects.
- Rna sequences are loaded into RGD_SEQUENCES table with seq_type set to 'ncbi_rna'.
- Sometimes NCBI provides new rna sequence for given transcript.
We load the new rna sequence, and keep the old one with seq_type set to 'old_ncbi_rna'.
Therefore we can have the whole history of changes for transcript reference rna sequence.

PROTEIN SEQUENCES

- We load protein sequences for transcript objects.
- Protein sequences are loaded into RGD_SEQUENCES table with seq_type set to 'ncbi_protein'.
- Sometimes NCBI provides new protein sequence for given transcript.
We load the new protein sequence, and keep the old one with seq_type set to 'old_ncbi_protein'.
Therefore we can have the whole history of changes for transcript reference protein sequence.
