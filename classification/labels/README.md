This directory contains all the predefined data label definitions used for 
classification. The label metadata is specified in the 
[`labels.yaml`](labels.yaml) file, and the classification rules are defined in
individual Rego files for each label.

To add a new predefined label, add its metadata to [`labels.yaml`](labels.yaml)
(following the file's instructions), as well as a corresponding classification
rule Rego file.

## Classification Rule Rego Files

Each label has a corresponding Rego file that defines the classification rule
for that label. The Rego file should be named after the label, with the `.rego`
extension. For example, the classification rule for the label `first_name`
should be defined in a file named `first_name.rego`.

The package for the rule should be named `classifier_<label>`, where `<label>`
is the name of the label in lowercase. For example, the package for the
classification rule for the label `first_name` should be named
`classifier_first_name`.

Rules should also have tests defined in a file named `<label>_test.rego`.

All Rego files (including tests) should be linted using [`regal`](https://www.openpolicyagent.org/integrations/regal/) 
to ensure they are formatted correctly, e.g.

```bash
$ regal lint /path/to/label.rego
```

### Input and Output

The input data for a classification rule is a JSON object containing the data
to be classified. This often represents a database table sample, for example.
The key names in the input data object correspond to the column names in the
database table, and the values are the sampled data in the table. For example,
input data representing a data sample from a database table called `users`
might look like this:

```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "john.doe@example.com"
}
```

Each rule must define an output variable named `output`, which must an 
[object](https://www.openpolicyagent.org/docs/latest/policy-language/#objects)
of the form:

```json
{
  "key": boolean
}
```

where `key` is each key from the input data, and `boolean` is a boolean value
indicating whether the key is classified as the label or not. For example, the
output object for the `first_name` label using the example input data above
would look like this:

```json
{
  "first_name": true,
  "last_name": false,
  "email": false
}
```

See this example on the [Rego Playground](https://play.openpolicyagent.org/p/niTDt5JwN8).

Please see the existing classification rules and their tests for examples of how
to write classification rules.
