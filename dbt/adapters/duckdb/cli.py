#!/usr/bin/env python
import cmd
import os
import shlex
import sys
from typing import List
from typing import Optional

# Try to import iterfzf, but don't fail if it's not available
try:
    from iterfzf import iterfzf

    HAS_ITERFZF = True
except ImportError:
    HAS_ITERFZF = False

from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.cli.main import dbtRunner
from dbt.cli.main import dbtRunnerResult


class DuckdbtShell(cmd.Cmd):
    intro = "Welcome to the duckdbt shell. Type help or ? to list commands."
    prompt = "duckdbt> "

    def __init__(self, profile: Optional[str] = None):
        super().__init__()
        self.dbt = dbtRunner()
        self.profile = profile

        # Run debug to test out the connection on startup
        result = self.dbt.invoke(["debug"])
        if result.success:
            # Parse the manifest
            res: dbtRunnerResult = self.dbt.invoke(["parse"])
            if res.success:
                self.manifest = res.result
                self._update_model_names_cache()

            if DuckDBConnectionManager._ENV:
                env = DuckDBConnectionManager._ENV
                cursor = env.handle().cursor()

                # Check if .duckdbrc file exists
                # and execute the SQL commands if it does
                duckdbrc_path = os.path.expanduser("~/.duckdbrc")
                if os.path.exists(duckdbrc_path):
                    print(f"Executing commands from {duckdbrc_path}...")
                    try:
                        with open(duckdbrc_path, "r") as f:
                            sql_script = f.read()
                            if sql_script.strip():
                                cursor.execute(sql_script)
                                print(f"Successfully executed {duckdbrc_path}")
                    except Exception as e:
                        print(f"Error executing {duckdbrc_path}: {e}")

                print("Launching DuckDB UI...")
                cursor.execute("CALL start_ui()")
                cursor.close()
        else:
            sys.exit(1)

        project_name = os.path.basename(os.getcwd())
        self.prompt = f"duckdbt ({project_name})> "

    def _run_dbt_command(self, command: List[str]) -> Optional[dbtRunnerResult]:
        """Run a dbt command with the runner"""
        cmd_args = []

        # Add profile if specified
        if self.profile:
            cmd_args.extend(["--profile", self.profile])

        # Add the user command
        cmd_args.extend(command)

        try:
            result: dbtRunnerResult = self.dbt.invoke(cmd_args)
            if result.success:
                return result
            else:
                print("Command failed: ", result.exception)
                return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    def do_run(self, arg):
        """Run models: run [options]"""
        args = ["run"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_test(self, arg):
        """Test models: test [options]"""
        args = ["test"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_build(self, arg):
        """Build models: build [options]"""
        args = ["build"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_seed(self, arg):
        """Load seed files: seed [options]"""
        args = ["seed"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_snapshot(self, arg):
        """Run snapshots: snapshot [options]"""
        args = ["snapshot"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_compile(self, arg):
        """Compile models: compile [options]"""
        args = ["compile"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_debug(self, arg):
        """Debug connection: debug [options]"""
        args = ["debug"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_deps(self, arg):
        """Install dependencies: deps [options]"""
        args = ["deps"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_list(self, arg):
        """List resources: list [options]"""
        args = ["list"] + shlex.split(arg)
        self._run_dbt_command(args)

    def do_parse(self, arg):
        """Parse project: parse [options]"""
        args = ["parse"] + shlex.split(arg)
        result = self._run_dbt_command(args)
        if result and result.success:
            self.manifest = result.result
            self._update_model_names_cache()

    def _update_model_names_cache(self):
        """Update the cached list of model names from the manifest"""
        if not self.manifest:
            self.model_names_cache = []
            return

        model_names = []
        # Get names of all models from the manifest
        for node_name, node in self.manifest.nodes.items():
            if node.resource_type in ("model", "seed", "snapshot", "source"):
                # Use the name without the project prefix
                model_names.append(node.name)

        self.model_names_cache = sorted(model_names)

    def do_exit(self, arg):
        """Exit the shell"""
        print("Goodbye!")
        return True

    def do_quit(self, arg):
        """Exit the shell"""
        return self.do_exit(arg)

    def do_EOF(self, arg):
        """Exit on Ctrl-D"""
        print()  # Print newline
        return self.do_exit(arg)

    def emptyline(self):
        """Do nothing on empty line"""
        pass

    def completedefault(self, text, line, begidx, endidx):
        """Default completion handler for all commands"""
        # Check if the text contains the ** trigger
        if not HAS_ITERFZF:
            print(
                "\nTo use model autocomplete (** pattern), please install iterfzf: pip install iterfzf"
            )
        elif not self.model_names_cache:
            pass
        else:
            query_prefix = text.split()[-1]
            try:
                # Use fzf for interactive selection
                models = [x for x in self.model_names_cache if x.startswith(query_prefix)]
                selected = iterfzf(models)

                # Return the selected model
                if selected:
                    return [selected]
            except Exception as e:
                print(f"\nError with fzf completion: {e}")

        # Fall back to cmd's default completion behavior
        return super().completedefault(text, line, begidx, endidx)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="duckdbt interactive shell")
    parser.add_argument(
        "--profile",
        help="Name of the dbt profile to use",
    )

    args = parser.parse_args()

    shell = DuckdbtShell(
        profile=args.profile,
    )
    shell.cmdloop()


if __name__ == "__main__":
    main()
