package oracle

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cyralinc/dmap/discovery/repository/genericsql"
)

// Query Alias
const (
	usernameAlias         = "USERNAME"
	oracleMaintainedAlias = "ORACLE_MAINTAINED"
	statusAlias           = "ACCOUNT_STATUS"
	createdAtAlias        = "CREATED_AT"
	lastUsedAtAlias       = "LAST_USED_AT"
	roleAlias             = "ROLE"
	isPredefinedAlias     = "IS_PREDEFINED"
)

type OracleTestSuite struct {
	suite.Suite
	repo                              oracleRepository
	mock                              sqlmock.Sqlmock
	databaseUserQuery                 string
	databaseRolesForUserQueryTemplate string
}

func TestOracle(t *testing.T) {
	s := new(OracleTestSuite)
	suite.Run(t, s)
}

func (s *OracleTestSuite) SetupSuite() {
	s.databaseUserQuery = `
	SELECT 
		users.username AS ` + usernameAlias + `,
		users.oracle_maintained AS ` + oracleMaintainedAlias + `,
		users.account_status AS ` + statusAlias + `,
		users.created AS ` + createdAtAlias + `,
		users.last_login AS ` + lastUsedAtAlias + `
	FROM 
		dba_users users
	ORDER BY
		users.username
	ASC
		`
	s.databaseRolesForUserQueryTemplate = `
	SELECT 
		privs.granted_role AS ` + roleAlias + `,
		roles.oracle_maintained AS ` + isPredefinedAlias + `
	FROM 
		dba_role_privs privs
	LEFT JOIN
		dba_roles roles
	ON
		privs.granted_role = roles.role
	WHERE 
		privs.grantee = '%s'
	ORDER BY
		privs.granted_role
	ASC
		`
}

func (s *OracleTestSuite) BeforeTest(suiteName, testName string) {
	db, mock, err := sqlmock.New()
	require.NoError(s.T(), err)

	s.mock = mock
	s.repo = oracleRepository{
		genericSqlRepo: genericsql.NewGenericSqlRepositoryFromDB(
			"oracle-repo",
			"oracle-driver",
			"oracle-db",
			db,
		),
	}
}

func (s *OracleTestSuite) TearDownTest() {
	s.repo.genericSqlRepo.GetDb().Close()
}
