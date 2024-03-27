package classifier_ssn

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, val) := "SSN" if {
	pattern := regex.find_n(`\b(\d{9}|\d{3}\-\d{2}\-\d{4})\b`, val, 1)
	count(pattern) == 1
	t_val := replace(pattern[0], "-", "")
	f_check := substring(t_val, 0, 3)
	f_check != "000"
	f_check != "666"
	to_number(f_check) < 900
	m_check := substring(t_val, 3, 2)
	m_check != "00"
	e_check := substring(t_val, 5, 4)
	e_check != "0000"
}
