package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"

	"github.com/jackc/pgx/v5"
	neon "github.com/kislerdm/neon-sdk-go"
	"github.com/spf13/cobra"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	RollbackMode              bool
	MainConnectionURL         string
	TableName                 string
	TableConnectionColumn     string
	TableIDColumn             string
	TableBranchColumn         string
	MigrateCommandName        string
	RollbackCommandName       string
	TenantSchemaConnectionURL string
	NeonRoleName              string
	NeonRolePassword          string
	NeonAPIKey                string
	NeonProjectID             string
	NeonDatabaseName          string
)

type tenant struct {
	ID     any
	Branch string
}

type migrationProcess struct {
	Success bool
	Tenants []tenant
}

func loadState() (migrationProcess, error) {
	config := migrationProcess{}
	b, err := os.ReadFile("state.msgpack")
	if err == nil {
		// Attempt to unmarshal using msgpack.
		if err = msgpack.Unmarshal(b, &config); err != nil {
			return config, fmt.Errorf("failed to decode the state: %v", err)
		}
	} else {
		if !os.IsNotExist(err) {
			// We should throw this to the user.
			return config, fmt.Errorf("failed to attempt loading state: %v", err)
		}
		if RollbackMode {
			// Return this error to the user.
			return config, err
		}
	}
	return config, nil
}

func (m *migrationProcess) write() {
	b, err := msgpack.Marshal(m)
	if err != nil {
		// AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA this was valid earlier??? This is panic worthy, something is awfully off
		// data integrity wise.
		panic(err)
	}
	err = os.WriteFile("state.msgpack", b, 0o644)
	if err != nil {
		// We should exit here.
		fmt.Fprintf(os.Stderr, "failed to write state: %v\n", err)
		os.Exit(1)
	}
}

// Performs a shell command and in the event of failure returns the status (unless the command was totally broken).
// Sets CONNECTION_URL as a environment variable and prints to either stdout/stderr depending on what the application does.
func performShellCommand(cmd, connectionUrl string) int {
	// Find your shell.
	var shell string
	shell = os.Getenv("SHELL")
	if shell == "" {
		if runtime.GOOS == "windows" {
			shell = "powershell"
		} else {
			shell = "/bin/sh"
		}
	}

	// Setup the environment.
	env := os.Environ()[:]
	env = append(env, "CONNECTION_URL="+connectionUrl)

	// Run the command.
	execCmd := exec.Command(shell, "-c", cmd)
	execCmd.Env = env
	execCmd.Stderr = os.Stderr
	execCmd.Stdout = os.Stdout
	if err := execCmd.Start(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to start command: %v\n", err)
		return 1
	}

	// Wait for execution.
	if err := execCmd.Wait(); err != nil {
		execErr, _ := err.(*exec.ExitError)
		if execErr != nil {
			return execErr.ExitCode()
		}
		_, _ = fmt.Fprintf(os.Stderr, "failed to execute command: %v\n", err)
		return 1
	}
	return 0
}

// Handles performing rollbacks where required.
func performRollback(neonClient *neon.Client, mainDb *pgx.Conn, state migrationProcess) error {
	// Rollback the main schema.
	exitCode := performShellCommand(RollbackCommandName, TenantSchemaConnectionURL)
	if exitCode != 0 {
		return errors.New("failed to rollback main schema")
	}

	// Rollback the tenants in reverse. This is because we want to rollback near where the error occurred.
	for i := len(state.Tenants) - 1; i >= 0; i-- {
		tenant := state.Tenants[i]

		// Add compute to the branch.
		endpoint, err := neonClient.CreateProjectEndpoint(NeonProjectID, neon.EndpointCreateRequest{
			Endpoint: neon.EndpointCreateRequestEndpoint{
				BranchID: tenant.Branch,
				Type:     "read_write",
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create endpoint: %v", err)
		}

		// Create the connection URL.
		connectionUrl := fmt.Sprintf(
			"postgresql://%s:%s@%s/%s", url.PathEscape(NeonRoleName), url.PathEscape(NeonRolePassword), endpoint.Endpoint.Host,
			NeonDatabaseName,
		)

		// Update in the main database.
		sql := "UPDATE " + TableName + " SET " + TableBranchColumn + " = $1, " + TableConnectionColumn + " = $2 WHERE " + TableIDColumn + " = $3 RETURNING " + TableBranchColumn
		row := mainDb.QueryRow(context.Background(), sql, nil, connectionUrl, tenant.ID)
		var branch string
		if err := row.Scan(&branch); err != nil {
			return fmt.Errorf("failed to update main database: %v", err)
		}

		// If the branch isn't the same, delete it.
		if branch != tenant.Branch && branch != "" {
			if _, err = neonClient.DeleteProjectBranch(NeonProjectID, tenant.Branch); err != nil {
				return fmt.Errorf("failed to delete branch: %v", err)
			}
		}

		// Remove the tenant from the state.
		state.Tenants = append(state.Tenants[:i], state.Tenants[i+1:]...)

		// Write the state.
		state.write()
	}

	// Delete the state file.
	_ = os.Remove("state.msgpack")

	// Mark this as a success.
	fmt.Println("rollback successful")
	return nil
}

// The main command to handle migrations.
func migrate(_ *cobra.Command, _ []string) error {
	// Make the Neon client.
	neonClient, err := neon.NewClient(neon.Config{Key: NeonAPIKey})
	if err != nil {
		return err
	}

	// Attempt to connect to the main database.
	mainDb, err := pgx.Connect(context.Background(), MainConnectionURL)
	if err != nil {
		return fmt.Errorf("failed to connect to the main database: %v", err)
	}
	if err = mainDb.Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to ping the main database: %v", err)
	}
	defer mainDb.Close(context.Background())

	// Load the config from disk if applicable.
	state, err := loadState()
	if err != nil {
		return err
	}

	// Check if we are in rollback mode. If so, immediately return the rollback handler.
	if RollbackMode {
		return performRollback(neonClient, mainDb, state)
	}

	// Handle if we aren't starting from a clean slate.
	if !state.Success && len(state.Tenants) != 0 {
		fmt.Println("last migration was dirty - rolling that back before we do anything!")
		if err = performRollback(neonClient, mainDb, state); err != nil {
			return err
		}
	}
	state = migrationProcess{}

	// Firstly try to migrate the main schema.
	exitCode := performShellCommand(MigrateCommandName, TenantSchemaConnectionURL)
	if exitCode != 0 {
		// Attempt to rollback the migration.
		performShellCommand(RollbackCommandName, TenantSchemaConnectionURL)
		os.Exit(exitCode)
	}

	// Defines if this is successful. Any non-success case will be rolled back at the end.
	migrationSuccess := false

	// Defines the defer that handles final steps.
	defer func() {
		if migrationSuccess {
			// Oh sweet! Write this to the file with success set to true.
			state.Success = true
			state.write()
			return
		}

		// This isn't so good. Attempt to rollback.
		fmt.Println("something went wrong - performing rollback!")
		err := performRollback(neonClient, mainDb, state)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to rollback migrations: %v\n", err)
			os.Exit(1)
		}
	}()

	// Start migrating users now.
	fmt.Println("main schema migrated - starting to migrate user branches now!")
	sql := "SELECT " + TableIDColumn + ", " + TableBranchColumn + ", " + TableConnectionColumn + " FROM " + TableName
	rows, err := mainDb.Query(context.Background(), sql)
	if err != nil {
		return fmt.Errorf("failed to query main database: %v", err)
	}

	rowCount := 0
	for rows.Next() {
		// Get the ID, branch, and connection.
		rowCount++
		var tableId any
		var tableBranch, tableConnection string
		if err := rows.Scan(&tableId, &tableBranch, &tableConnection); err != nil {
			return fmt.Errorf("failed to get row from main database: %v", err)
		}

		// Create a branch in case anything goes wrong.
		branch, err := neonClient.CreateProjectBranch(NeonProjectID, &neon.CreateProjectBranchReqObj{
			BranchCreateRequest: neon.BranchCreateRequest{
				Branch: &neon.BranchCreateRequestBranch{
					ParentID: &tableBranch,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create neon branch: %v", err)
		}
		newBranchId := branch.Branch.ID

		// Write the branch in case of failure.
		state.Tenants = append(state.Tenants, tenant{
			ID:     tableId,
			Branch: newBranchId,
		})
		state.write()

		// Perform the migration.
		statusCode := performShellCommand(MigrateCommandName, tableConnection)
		if statusCode != 0 {
			return errors.New("migration finished with status code " + strconv.Itoa(statusCode))
		}
	}

	// Log how many rows we migrated and mark this as successful.
	fmt.Println(rowCount, "users migrated")
	migrationSuccess = true
	return nil
}

// The main entrypoint of the application.
func main() {
	mainCmd := cobra.Command{
		Use:   "neon-bulk-migrator",
		Short: "Handles bulk migrations of Neon databases",
		RunE:  migrate,
	}

	mainCmd.Flags().BoolVarP(&RollbackMode, "rollback-mode", "", false, "Defines if this is in rollback mode where the last state will be rolled back (defaults to false)")

	mainCmd.Flags().StringVarP(&MainConnectionURL, "main-connection-url", "", "", "Main Connection URL (required)")
	mainCmd.MarkFlagRequired("main-connection-url")

	mainCmd.Flags().StringVarP(&TableName, "table-name", "", "", "Raw SQL to handle Table Name (required)")
	mainCmd.MarkFlagRequired("table-name")

	mainCmd.Flags().StringVarP(&TableConnectionColumn, "table-connection-column", "", "", "Raw SQL to handle Table Connection Column (can use -> to handle JSON, required)")
	mainCmd.MarkFlagRequired("tenant-connection-column")

	mainCmd.Flags().StringVarP(&TableBranchColumn, "table-branch-column", "", "", "Raw SQL to handle Table Branch Column (required)")
	mainCmd.MarkFlagRequired("table-branch-column")

	mainCmd.Flags().StringVarP(&TableIDColumn, "table-id-column", "", "", "Raw SQL to handle Table ID Column (required)")
	mainCmd.MarkFlagRequired("table-id-column")

	mainCmd.Flags().StringVarP(&MigrateCommandName, "migrate-command-name", "", "", "Command to do migration - takes a CONNECTION_URL environment variable (required)")
	mainCmd.MarkFlagRequired("migrate-command-name")

	mainCmd.Flags().StringVarP(&RollbackCommandName, "rollback-command-name", "", "", "Command to do rollback - takes a CONNECTION_URL environment variable (required)")
	mainCmd.MarkFlagRequired("rollback-command-name")

	mainCmd.Flags().StringVarP(&TenantSchemaConnectionURL, "tenant-schema-connection-url", "", "", "Tenant Schema Connection URL (required)")
	mainCmd.MarkFlagRequired("tenant-schema-connection-url")

	mainCmd.Flags().StringVarP(&NeonRoleName, "neon-role-name", "", "", "Neon Role Name (required)")
	mainCmd.MarkFlagRequired("neon-role-name")

	mainCmd.Flags().StringVarP(&NeonRolePassword, "neon-role-password", "", "", "Neon Role Password (required)")
	mainCmd.MarkFlagRequired("neon-role-password")

	mainCmd.Flags().StringVarP(&NeonAPIKey, "neon-api-key", "", "", "Neon API Key (required)")
	mainCmd.MarkFlagRequired("neon-api-key")

	mainCmd.Flags().StringVarP(&NeonProjectID, "neon-project-id", "", "", "Neon Project ID (required)")
	mainCmd.MarkFlagRequired("neon-project-id")

	mainCmd.Flags().StringVarP(&NeonDatabaseName, "neon-database-name", "", "", "Neon Database Name (required)")
	mainCmd.MarkFlagRequired("neon-database-name")

	_ = mainCmd.Execute()
}
