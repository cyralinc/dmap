package classifier_phone_number

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, val) if {
	regex.match(
		`\A((1(-| )?((\([2-9]\d{2}\))|([2-9]\d{2})))|([2-9]\d{2})|(\([2-9]\d{2}\)))(-| )?[2-9]((1[02-9])|([02-9]\d))(-| )?\d{4}\z`,
		val
	)
}
