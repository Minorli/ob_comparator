# init-users-roles

## Purpose
Define how the online initializer creates users/roles and applies role/system privileges in OceanBase.

## ADDED Requirements

### Requirement: Config-driven initialization
The initializer SHALL read Oracle and OceanBase connection settings from config.ini and use the default path when no argument is provided.

#### Scenario: Missing config
- **WHEN** the config file cannot be loaded
- **THEN** the initializer exits with a configuration error

#### Scenario: Missing OB config fields
- **WHEN** required OCEANBASE_TARGET fields are missing
- **THEN** the initializer exits with a configuration error

### Requirement: Oracle source filtering
The initializer SHALL collect users and roles from Oracle where ORACLE_MAINTAINED = 'N'.

#### Scenario: Non-system principals collected
- **WHEN** ORACLE_MAINTAINED is supported by the source views
- **THEN** only rows with ORACLE_MAINTAINED = 'N' are included

### Requirement: Online creation in OceanBase
The initializer SHALL create missing roles and users in OceanBase and skip those already present.

#### Scenario: Role exists
- **WHEN** a role already exists in the target
- **THEN** the role creation is skipped

#### Scenario: User exists
- **WHEN** a user already exists in the target
- **THEN** the user creation is skipped

### Requirement: Fixed default password
The initializer SHALL assign the fixed password `Ob@sx2025` to created users.

#### Scenario: Create user password
- **WHEN** a new user is created
- **THEN** the CREATE USER statement uses `Ob@sx2025`

### Requirement: Role and system privilege grants
The initializer SHALL apply role grants and system privileges for non-system principals and ignore object privileges.

#### Scenario: Role grant
- **WHEN** a role is granted to a user or role in Oracle
- **THEN** a matching GRANT statement is generated and executed in OceanBase

#### Scenario: System privilege grant
- **WHEN** a system privilege is granted to a user or role in Oracle
- **THEN** a matching GRANT statement is generated and executed in OceanBase

#### Scenario: Object privilege ignored
- **WHEN** an object privilege exists in Oracle
- **THEN** it is not collected or applied by the initializer

### Requirement: Online execution
The initializer SHALL execute generated statements against OceanBase and continue after failures.

#### Scenario: Statement failure
- **WHEN** a statement fails during execution
- **THEN** the initializer logs the failure and continues with remaining statements

### Requirement: Local DDL persistence
The initializer SHALL write SQL files for create and grant statements under `fixup_scripts/init_users_roles`.

#### Scenario: DDL files written
- **WHEN** statements are generated
- **THEN** the SQL files are written under the init_users_roles output directory
