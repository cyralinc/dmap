package classifier_phone

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(input[k])
}

default classify(_) := false

classify(val) if {
	regex.match(
		`\A((1(-| )?((\([2-9]\d{2}\))|([2-9]\d{2})))|([2-9]\d{2})|(\([2-9]\d{2}\)))(-| )?[2-9]((1[02-9])|([02-9]\d))(-| )?\d{4}\z`,
		val,
	)
}
