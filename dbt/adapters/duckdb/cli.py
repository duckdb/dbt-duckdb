#!/usr/bin/env python
import cmd
import os
import shlex
import sys
from typing import List, Optional

from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.cli.main import dbtRunner, dbtRunnerResult


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
            cursor = DuckDBConnectionManager._ENV.handle().cursor()
            cursor.execute("CALL start_ui()")
            cursor.close()
        else:
            print("Exception running dbt debug...", result.exception)
            sys.exit(1)

        project_name = os.path.basename(os.getcwd())
        self.prompt = f"duckdbt ({project_name})> "

    def _run_dbt_command(self, command: List[str]) -> None:
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
                return
            else:
                print("Command failed: ", result.exception)
                return
        except Exception as e:
            print(f"Error: {str(e)}")

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
        self._run_dbt_command(args)

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
