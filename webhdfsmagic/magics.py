"""
IPython magic commands for WebHDFS/Knox operations.

This is the refactored version that delegates to specialized command modules.
"""

import json
import traceback
from typing import Any, Optional, Union

import pandas as pd
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.display import HTML
from traitlets import TraitType, Unicode

from .client import WebHDFSClient
from .commands import (
    CatCommand,
    ChmodCommand,
    ChownCommand,
    GetCommand,
    ListCommand,
    MkdirCommand,
    PutCommand,
    RmCommand,
)
from .config import ConfigurationManager
from .logger import get_logger


class BoolOrString(TraitType):
    """A trait for values that can be either a boolean or a string (for SSL certificate paths)."""

    info_text = "either a boolean or a string"

    def validate(self, obj, value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value
        self.error(obj, value)


@magics_class
class WebHDFSMagics(Magics):
    """
    IPython magics to interact with HDFS via WebHDFS/Knox.

    Supported commands:
      - %hdfs ls      : List files on HDFS
      - %hdfs mkdir   : Create a directory on HDFS
      - %hdfs rm      : Delete files/directories (wildcards supported)
      - %hdfs put     : Upload local files to HDFS
      - %hdfs get     : Download files from HDFS
      - %hdfs cat     : Display file content
      - %hdfs chmod   : Change file/directory permissions
      - %hdfs chown   : Change owner and group
    """

    # Configuration traits
    knox_url = Unicode("https://localhost:8443/gateway/default").tag(config=True)
    webhdfs_api = Unicode("/webhdfs/v1").tag(config=True)
    auth_user = Unicode().tag(config=True)
    auth_password = Unicode().tag(config=True)
    verify_ssl = BoolOrString(default_value=False).tag(config=True)

    def __init__(self, shell):
        """Initialize the extension and load configuration."""
        super().__init__(shell=shell)
        self.logger = get_logger()
        self.logger.info("Initializing WebHDFSMagics extension")
        self._load_external_config()
        self._initialize_client()
        self.logger.info("WebHDFSMagics extension initialized successfully")

    def _load_external_config(self):
        """Load configuration from config files."""
        config_manager = ConfigurationManager()
        config = config_manager.load()

        self.knox_url = config["knox_url"]
        self.webhdfs_api = config["webhdfs_api"]
        self.auth_user = config["auth_user"]
        self.auth_password = config["auth_password"]
        self.verify_ssl = config["verify_ssl"]

    def _initialize_client(self):
        """Initialize WebHDFS client and command objects."""
        self.client = WebHDFSClient(
            knox_url=self.knox_url,
            webhdfs_api=self.webhdfs_api,
            auth_user=self.auth_user,
            auth_password=self.auth_password,
            verify_ssl=self.verify_ssl,
        )

        # Initialize command objects
        self.list_cmd = ListCommand(self.client)
        self.mkdir_cmd = MkdirCommand(self.client)
        self.rm_cmd = RmCommand(self.client)
        self.cat_cmd = CatCommand(self.client)
        self.get_cmd = GetCommand(self.client)
        self.put_cmd = PutCommand(self.client)
        self.chmod_cmd = ChmodCommand(self.client)
        self.chown_cmd = ChownCommand(self.client)

    def _format_ls(self, path: str) -> Union[pd.DataFrame, dict]:
        """
        Format directory listing.
        Wrapper for backward compatibility with tests.
        """
        return self.list_cmd.execute(path)

    def _execute(self, method: str, operation: str, path: str, **params) -> dict[str, Any]:
        """
        Execute a WebHDFS request.
        Wrapper for backward compatibility with tests.
        """
        return self.client.execute(method, operation, path, **params)

    def _set_permission(self, path: str, permission: str) -> str:
        """Set permissions (backward compatibility)."""
        return self.chmod_cmd._set_permission(path, permission)

    def _set_owner(self, path: str, owner: str, group: Optional[str] = None) -> str:
        """Set owner (backward compatibility)."""
        return self.chown_cmd._set_owner(path, owner, group)

    @line_magic
    def hdfs(self, line: str) -> Union[pd.DataFrame, str, HTML]:
        """
        Main entry point for %hdfs magic commands.

        Args:
            line: Command line entered by user

        Returns:
            Command result or HTML help
        """
        parts = line.strip().split()
        if not parts:
            return self._help()

        cmd = parts[0].lower()
        args = parts[1:]

        # Log command execution
        self.logger.log_operation_start(
            operation=f"hdfs {cmd}",
            command=line,
            args=args,
        )

        try:
            if cmd == "help":
                result = self._help()
                self.logger.log_operation_end(operation="hdfs help", success=True)
                return result

            elif cmd == "setconfig":
                result = self._handle_setconfig(args)
                self.logger.log_operation_end(operation="hdfs setconfig", success=True)
                return result

            elif cmd == "ls":
                path = args[0] if args else "/"
                result = self.list_cmd.execute(path)
                self.logger.log_operation_end(
                    operation="hdfs ls",
                    success=True,
                    path=path,
                    file_count=len(result) if isinstance(result, pd.DataFrame) else 0,
                )
                # Handle empty directory case
                if isinstance(result, dict) and result.get("empty_dir"):
                    return result
                return result

            elif cmd == "mkdir":
                path = args[0]
                result = self.mkdir_cmd.execute(path)
                self.logger.log_operation_end(
                    operation="hdfs mkdir", success=True, path=path
                )
                return result

            elif cmd == "rm":
                result = self._handle_rm(args)
                self.logger.log_operation_end(
                    operation="hdfs rm", success=True, pattern=args[0] if args else None
                )
                return result

            elif cmd == "put":
                if len(args) < 2:
                    return "Usage: %hdfs put <local_pattern> <hdfs_dest>"
                result = self.put_cmd.execute(args[0], args[1])
                self.logger.log_operation_end(
                    operation="hdfs put",
                    success=True,
                    local_pattern=args[0],
                    hdfs_dest=args[1],
                )
                return result

            elif cmd == "get":
                if len(args) < 2:
                    return "Usage: %hdfs get <hdfs_source> <local_dest>"
                result = self.get_cmd.execute(args[0], args[1], self._format_ls)
                self.logger.log_operation_end(
                    operation="hdfs get",
                    success=True,
                    hdfs_source=args[0],
                    local_dest=args[1],
                )
                return result

            elif cmd == "cat":
                result = self._handle_cat(args)
                self.logger.log_operation_end(
                    operation="hdfs cat", success=True, file=args[0] if args else None
                )
                return result

            elif cmd == "chmod":
                result = self._handle_chmod(args)
                self.logger.log_operation_end(operation="hdfs chmod", success=True)
                return result

            elif cmd == "chown":
                result = self._handle_chown(args)
                self.logger.log_operation_end(operation="hdfs chown", success=True)
                return result

            else:
                error_msg = f"Unknown command: {cmd}. Type '%hdfs help' for available commands."
                self.logger.warning(f"Unknown command attempted: {cmd}")
                return error_msg

        except Exception as e:
            self.logger.log_error(operation=f"hdfs {cmd}", error=e, command=line, args=args)
            tb = traceback.format_exc()
            return f"Error: {str(e)}\nTraceback:\n{tb}"

    def _handle_setconfig(self, args: list) -> str:
        """Handle setconfig command."""
        if not args:
            return (
                "Usage: %hdfs setconfig <json_config>\n"
                'Example: %hdfs setconfig {"knox_url": "https://...", ...}'
            )
        config_str = " ".join(args)
        try:
            config = json.loads(config_str)
            self.knox_url = config.get("knox_url", self.knox_url)
            self.webhdfs_api = config.get("webhdfs_api", self.webhdfs_api)
            self.auth_user = config.get("username", self.auth_user)
            self.auth_password = config.get("password", self.auth_password)
            self.verify_ssl = config.get("verify_ssl", self.verify_ssl)
            # Reinitialize client with new configuration
            self._initialize_client()
            return "Configuration successfully updated."
        except json.JSONDecodeError as e:
            return f"Error parsing JSON: {str(e)}"

    def _handle_cat(self, args: list) -> str:
        """Handle cat command with argument parsing."""
        if not args:
            return "Usage: %hdfs cat <file> [-n <number_of_lines>]"

        file_path = None
        num_lines = 100

        i = 0
        while i < len(args):
            if args[i] == "-n":
                if i + 1 >= len(args):
                    return "Error: -n option requires a number argument."
                try:
                    num_lines = int(args[i + 1])
                    i += 2
                except ValueError:
                    return f"Error: invalid number of lines '{args[i + 1]}'."
            else:
                if file_path is not None:
                    return "Error: multiple file paths specified."
                file_path = args[i]
                i += 1

        if not file_path:
            return "Usage: %hdfs cat <file> [-n <number_of_lines>]"

        return self.cat_cmd.execute(file_path, num_lines)

    def _handle_chmod(self, args: list) -> str:
        """Handle chmod command."""
        recursive = False
        arg_index = 0

        if args[0] == "-R":
            recursive = True
            arg_index = 1

        permission = args[arg_index]
        path = args[arg_index + 1]

        return self.chmod_cmd.execute(path, permission, recursive, self._format_ls)

    def _handle_chown(self, args: list) -> str:
        """Handle chown command."""
        recursive = False
        arg_index = 0

        if args[0] == "-R":
            recursive = True
            arg_index = 1

        owner_group = args[arg_index]
        path = args[arg_index + 1]

        if ":" in owner_group:
            owner, group = owner_group.split(":", 1)
        else:
            owner = owner_group
            group = None

        return self.chown_cmd.execute(path, owner, group, recursive, self._format_ls)

    def _handle_rm(self, args: list) -> str:
        """Handle rm command using RmCommand."""
        recursive_flag = "-r" in args or "-R" in args
        if recursive_flag:
            args = [a for a in args if a not in ["-r", "-R"]]

        pattern = args[0]
        return self.rm_cmd.execute(pattern, recursive_flag, self._format_ls)

    def _help(self) -> HTML:
        """Display help information."""
        html = """
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr>
                    <th>Command</th>
                    <th>Description</th>
                </tr>
            </thead>
            <tbody>
                <tr><td>%hdfs help</td><td>Display this help</td></tr>
                <tr><td>%hdfs setconfig {...}</td><td>Set configuration</td></tr>
                <tr><td>%hdfs ls [path]</td><td>List files</td></tr>
                <tr><td>%hdfs mkdir &lt;path&gt;</td><td>Create directory</td></tr>
                <tr><td>%hdfs rm &lt;path&gt; [-r]</td>
                    <td>Delete file/directory</td></tr>
                <tr><td>%hdfs put &lt;local&gt; &lt;hdfs&gt;</td>
                    <td>Upload files</td></tr>
                <tr><td>%hdfs get &lt;hdfs&gt; &lt;local&gt;</td>
                    <td>Download files</td></tr>
                <tr><td>%hdfs cat &lt;file&gt; [-n &lt;lines&gt;]</td>
                    <td>Display file content</td></tr>
                <tr><td>%hdfs chmod [-R] &lt;perm&gt; &lt;path&gt;</td>
                    <td>Change permissions</td></tr>
                <tr><td>%hdfs chown [-R] &lt;user:group&gt; &lt;path&gt;</td>
                    <td>Change owner</td></tr>
            </tbody>
        </table>
        """
        return HTML(html)


def load_ipython_extension(ipython):
    """Register the IPython extension."""
    ipython.register_magics(WebHDFSMagics)
