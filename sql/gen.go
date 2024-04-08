package sql

// Mock generation - see https://vektra.github.io/mockery/

//go:generate mockery --with-expecter --srcpkg=github.com/cyralinc/dmap/classification --name=Classifier --structname=MockClassifier --output=. --outpkg=sql --filename=mock_classifier_test.go
//go:generate mockery --with-expecter --inpackage --name=Repository --filename=mock_repository_test.go
