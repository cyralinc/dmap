package classifier_dob_test

import data.classifier_dob
import rego.v1

test_dob_key if {
	classifier_dob.output.dob == true with input as {"dob": "test"}
	classifier_dob.output.DOB == true with input as {"DOB": "test"}
	classifier_dob.output.DoB == true with input as {"DoB": "test"}
	classifier_dob.output.birthdate == true with input as {"birthdate": "test"}
	classifier_dob.output.BirthDate == true with input as {"BirthDate": "test"}
	classifier_dob.output.dateofbirth == true with input as {"dateofbirth": "test"}
	classifier_dob.output.DateOfBirth == true with input as {"DateOfBirth": "test"}
	classifier_dob.output.birthdate == true with input as {"birthdate": "test"}
	classifier_dob.output.birth_date == true with input as {"birth_date": "test"}
}

test_dob_pattern_month_day_year if {
	classifier_dob.output.message == true with input as {"message": "01/01/1900"}
	classifier_dob.output.message == true with input as {"message": "1-1-1900"}
	classifier_dob.output.message == true with input as {"message": "10.01.1971"}
	classifier_dob.output.message == true with input as {"message": "11/30/2023"}
	classifier_dob.output.message == true with input as {"message": "01/01/1900"}
	classifier_dob.output.message == true with input as {"message": "1/1/1900"}
	classifier_dob.output.message == true with input as {"message": "10/01/1971"}
	classifier_dob.output.message == true with input as {"message": "30/11/2023"}
}

test_dob_pattern_year_month_day if {
	classifier_dob.output.message == true with input as {"message": "1900/01/10"}
	classifier_dob.output.message == true with input as {"message": "1900/1/1"}
	classifier_dob.output.message == true with input as {"message": "1971/10/01"}
	classifier_dob.output.message == true with input as {"message": "2023/12/31"}
	classifier_dob.output.message == true with input as {"message": "1900-01-10"}
	classifier_dob.output.message == true with input as {"message": "1900-1-1"}
	classifier_dob.output.message == true with input as {"message": "1971-10-01"}
	classifier_dob.output.message == true with input as {"message": "2023-12-31"}
}
