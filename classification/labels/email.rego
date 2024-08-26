package classifier_email

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
		`\A[A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9]((\.[A-Za-z0-9])|(-[A-Za-z0-9])|[A-Za-z0-9])*\.[A-Za-z]{2,}\z`,
		val,
	)
}
