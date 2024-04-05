This directory contains all the predefined data label definitions used for 
classification. The label metadata is specified in the 
[`labels.yaml`](labels.yaml) file, and the classification rules are defined in
individual Rego files for each label.

To add a new predefined label, add its metadata to [`labels.yaml`](labels.yaml)
(following the file's instructions), as well as a corresponding classification
rule Rego file.
