package classifier_cvv

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "CVV" if {
	lower(key) == "cvv"
}

classify(key, _) := "CVV" if {
	lower(key) == "csc"
}

classify(key, _) := "CVV" if {
	lower(key) == "3csc"
}

classify(key, _) := "CVV" if {
	lower(key) == "cvc"
}

classify(key, _) := "CVV" if {
	lower(key) == "cav"
}

classify(key, _) := "CVV" if {
	lower(key) == "cid"
}

classify(key, _) := "CVV" if {
	lower(key) == "cvd"
}

classify(key, _) := "CVV" if {
	lower(key) == "cve"
}

classify(key, _) := "CVV" if {
	lower(key) == "cvn"
}

classify(key, _) := "CVV" if {
	lower(key) == "spc"
}

classify(key, _) := "CVV" if {
	lower(key) == "pvv"
}

classify(_, val) := "CVV" if {
	regex.match(`^\d{3,4}$`, val)
}
