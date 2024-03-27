package classifier_email

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(_, val) if {
	regex.match(
		`\A[A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9]((\.[A-Za-z0-9])|(-[A-Za-z0-9])|[A-Za-z0-9])*\.[A-Za-z]{2,}\z`,
		val
	)
}
