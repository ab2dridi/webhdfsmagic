"""
IPython magic commands for WebHDFS/Knox operations.

This is the refactored version that delegates to specialized command modules.
"""

import json
import traceback
from typing import Any, Optional, Union

import pandas as pd
import requests
from IPython.core.magic import Magics, line_magic, magics_class
from IPython.display import HTML
from traitlets import TraitType, Unicode

from .client import WebHDFSClient
from .commands import (
    CatCommand,
    ChmodCommand,
    ChownCommand,
    DuCommand,
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
      - %hdfs help      : Display detailed help with all commands and options
      - %hdfs setconfig : Set configuration (JSON format)
      - %hdfs ls        : List files on HDFS
      - %hdfs mkdir     : Create a directory on HDFS
      - %hdfs rm        : Delete files/directories (wildcards supported, -r for recursive)
    - %hdfs put       : Upload local files to HDFS (wildcards supported, -t/--threads for parallel)
    - %hdfs get       : Download files from HDFS (wildcards supported, -t/--threads for parallel)
      - %hdfs cat       : Smart preview with auto-detection (CSV/TSV/Parquet)
                          Options: -n <lines>, --format <type>, --raw
      - %hdfs chmod     : Change file/directory permissions (-R for recursive)
      - %hdfs chown     : Change owner and group (-R for recursive)
      - %hdfs du        : Show disk usage (real directory sizes via GETCONTENTSUMMARY)
                          Options: -s (summary only), -h (human-readable sizes)
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
        self.du_cmd = DuCommand(self.client)
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

    def _handle_http_error(
        self,
        error: requests.exceptions.HTTPError,
        path: str,
        context: str = "path",
    ) -> Optional[str]:
        """
        Handle HTTP errors with user-friendly messages.

        Args:
            error: The HTTPError exception
            path: The HDFS path being accessed
            context: Context type (e.g., 'directory', 'path', 'file')

        Returns:
            User-friendly error message if handled, None to re-raise
        """
        if error.response.status_code == 404:
            return f"{context.capitalize()} not found: {path}"
        return None

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

    def _extract_threads_option(self, args: list) -> tuple:
        """
        Extract --threads or -t option from arguments.
        
        Args:
            args: Command arguments list
            
        Returns:
            Tuple of (threads_count, remaining_args)
            Default threads_count is 1 if not specified
        """
        threads = 1
        remaining_args = []
        i = 0
        while i < len(args):
            if args[i] in ["-t", "--threads"] and i + 1 < len(args):
                try:
                    threads = int(args[i + 1])
                    if threads < 1:
                        threads = 1
                    i += 2
                except (ValueError, IndexError):
                    # Ignore invalid value, skip both option and value
                    i += 2
            else:
                remaining_args.append(args[i])
                i += 1
        return threads, remaining_args

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
                try:
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
                except requests.exceptions.HTTPError as e:
                    error_msg = self._handle_http_error(e, path, "directory")
                    if error_msg:
                        self.logger.log_operation_end(
                            operation="hdfs ls",
                            success=False,
                            path=path,
                            error=error_msg,
                        )
                        return error_msg
                    raise

            elif cmd == "du":
                result = self._handle_du(args)
                self.logger.log_operation_end(
                    operation="hdfs du",
                    success=True,
                    path=args[-1] if args else None,
                )
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
                    return "Usage: %hdfs put [-t <threads>] <local_pattern> <hdfs_dest>"
                threads, parsed_args = self._extract_threads_option(args)
                if len(parsed_args) < 2:
                    return "Usage: %hdfs put [-t <threads>] <local_pattern> <hdfs_dest>"
                result = self.put_cmd.execute(parsed_args[0], parsed_args[1], threads)
                self.logger.log_operation_end(
                    operation="hdfs put",
                    success=True,
                    local_pattern=parsed_args[0],
                    hdfs_dest=parsed_args[1],
                    threads=threads,
                )
                if isinstance(result, str) and result.count("\n") > 1:
                    print(result)
                    return
                return result


            elif cmd == "get":
                if len(args) < 2:
                    return "Usage: %hdfs get [-t <threads>] <hdfs_source> <local_dest>"
                threads, parsed_args = self._extract_threads_option(args)
                if len(parsed_args) < 2:
                    return "Usage: %hdfs get [-t <threads>] <hdfs_source> <local_dest>"
                result = self.get_cmd.execute(parsed_args[0], parsed_args[1], self._format_ls, threads)
                self.logger.log_operation_end(
                    operation="hdfs get",
                    success=True,
                    hdfs_source=parsed_args[0],
                    local_dest=parsed_args[1],
                    threads=threads,
                )
                if isinstance(result, str) and result.count("\n") > 1:
                    print(result)
                    return
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

    def _handle_du(self, args: list) -> Union[pd.DataFrame, dict, str]:
        """Handle du command with -s and -h option parsing."""
        if not args:
            return "Usage: %hdfs du [-s] [-h] <path>"

        summary = False
        human_readable = False
        path = None

        for arg in args:
            if arg == "-s":
                summary = True
            elif arg == "-h":
                human_readable = True
            elif arg in ("-sh", "-hs"):
                summary = True
                human_readable = True
            else:
                path = arg

        if not path:
            return "Usage: %hdfs du [-s] [-h] <path>"

        result = self.du_cmd.execute(path, summary=summary, human_readable=human_readable)
        if isinstance(result, dict) and result.get("empty_dir"):
            return result
        return result

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
            return "Usage: %hdfs cat <file> [-n <lines>] [--format <type>] [--raw]"

        file_path = None
        num_lines = 100
        format_type = None
        raw = False

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
            elif args[i] == "--format":
                if i + 1 >= len(args):
                    return (
                        "Error: --format option requires a type "
                        "(csv, parquet, pandas, polars, raw)."
                    )
                format_type = args[i + 1]
                valid_formats = ['csv', 'parquet', 'pandas', 'polars', 'raw']
                if format_type not in valid_formats:
                    formats_str = ', '.join(valid_formats)
                    return f"Error: invalid format type '{format_type}'. Use: {formats_str}."
                i += 2
            elif args[i] == "--raw":
                raw = True
                i += 1
            else:
                if file_path is not None:
                    return "Error: multiple file paths specified."
                file_path = args[i]
                i += 1

        if not file_path:
            return "Usage: %hdfs cat <file> [-n <lines>] [--format <type>] [--raw]"

        result = self.cat_cmd.execute(file_path, num_lines, format_type=format_type, raw=raw)

        # If result starts with "Error:", return it so it can be checked in tests
        # Otherwise print it for nice display in notebooks
        if result.startswith("Error:"):
            return result
        else:
            print(result)

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
        <style>
            .hdfs-help {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI",
                    Helvetica, Arial, sans-serif;
            }
            .hdfs-help table {
                border-collapse: collapse;
                width: 100%;
                margin: 10px 0;
            }
            .hdfs-help th {
                background-color: #f6f8fa;
                padding: 8px;
                text-align: left;
                border: 1px solid #d0d7de;
            }
            .hdfs-help td {
                padding: 8px;
                border: 1px solid #d0d7de;
                vertical-align: top;
            }
            .hdfs-help code {
                background-color: #f6f8fa;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
                font-size: 0.9em;
            }
            .hdfs-help .option {
                color: #0969da;
                font-weight: 500;
            }
        </style>
        <div class="hdfs-help">
        <h3>📖 WebHDFS Magic Commands</h3>
        <table>
            <thead>
                <tr>
                    <th style="width: 40%;">Command</th>
                    <th>Description</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><code>%hdfs help</code></td>
                    <td>Display this help</td>
                </tr>
                <tr>
                    <td><code>%hdfs setconfig {...}</code></td>
                    <td>Set configuration (JSON format)</td>
                </tr>
                <tr>
                    <td><code>%hdfs ls [path]</code></td>
                    <td>List files and directories</td>
                </tr>
                <tr>
                    <td><code>%hdfs mkdir &lt;path&gt;</code></td>
                    <td>Create directory</td>
                </tr>
                <tr>
                    <td><code>%hdfs du [options] &lt;path&gt;</code></td>
                    <td><strong>Disk usage</strong> — real directory sizes
                        (GETCONTENTSUMMARY)<br>
                        Returns a DataFrame with size, space_consumed,
                        file_count, dir_count<br>
                        <span class="option">-s</span> :
                        summary of path itself (no children iteration)<br>
                        <span class="option">-h</span> :
                        human-readable sizes (KB/MB/GB)<br>
                        <span class="option">-sh</span> :
                        combine both options</td>
                </tr>
                <tr>
                    <td><code>%hdfs rm &lt;path&gt; [-r]</code></td>
                    <td>Delete file/directory<br>
                        <span class="option">-r</span> : recursive deletion</td>
                </tr>
                <tr>
                    <td><code>%hdfs put &lt;local&gt; &lt;hdfs&gt;</code></td>
                    <td>Upload files (supports wildcards)<br>
                        <span class="option">-t, --threads &lt;N&gt;</span> :
                        use N parallel threads for multi-file uploads</td>
                </tr>
                <tr>
                    <td><code>%hdfs get &lt;hdfs&gt; &lt;local&gt;</code></td>
                    <td>Download files (supports wildcards)<br>
                        <span class="option">-t, --threads &lt;N&gt;</span> :
                        use N parallel threads for multi-file downloads</td>
                </tr>
                <tr>
                    <td><code>%hdfs cat &lt;file&gt; [options]</code></td>
                    <td><strong>Smart file preview</strong> (CSV/TSV/Parquet)<br>
                        <span class="option">-n &lt;lines&gt;</span> :
                        limit to N rows (default: 100)<br>
                        <span class="option">--format &lt;type&gt;</span> :
                        force format (csv, parquet, pandas, polars, raw)<br>
                        <span class="option">--raw</span> :
                        display raw content without formatting<br>
                        <br>
                        <strong>Auto-detects:</strong> file format, delimiter,
                        data types<br>
                        <strong>Formats:</strong>
                        <em>pandas</em> (classic),
                        <em>polars</em> (with schema),
                        <em>grid</em> (default table)</td>
                </tr>
                <tr>
                    <td><code>%hdfs chmod [-R] &lt;mode&gt; &lt;path&gt;</code></td>
                    <td>Change permissions (e.g., 644, 755)<br>
                        <span class="option">-R</span> : recursive</td>
                </tr>
                <tr>
                    <td><code>%hdfs chown [-R] &lt;user:group&gt; &lt;path&gt;</code></td>
                    <td>Change owner and group<br>
                        <span class="option">-R</span> : recursive</td>
                </tr>
            </tbody>
        </table>
        <p><strong>💡 Examples:</strong></p>
        <ul>
            <li><code>%hdfs cat data.csv -n 10</code> - Preview first 10 rows</li>
            <li><code>%hdfs cat data.parquet --format pandas</code> -
                Display in pandas format (classic)</li>
            <li><code>%hdfs cat data.parquet --format polars</code> -
                Display with schema and types</li>
            <li><code>%hdfs put *.csv /data/</code> - Upload all CSV files</li>
            <li><code>%hdfs put -t 4 ./data/*.csv /hdfs/input/</code> - 
                Upload files with 4 parallel threads</li>
            <li><code>%hdfs get -t 8 /hdfs/output/*.parquet ./results/</code> -
                Download files with 8 parallel threads</li>
            <li><code>%hdfs du /data/users</code> - Size of each user directory</li>
            <li><code>%hdfs du -h /data/users</code> - Same, human-readable sizes</li>
            <li><code>%hdfs du -sh /data/warehouse</code> - Total size of warehouse</li>
            <li><code>%hdfs chmod -R 755 /mydir</code> - Set permissions recursively</li>
        </ul>
        </div>
        """
        return HTML(html)


def load_ipython_extension(ipython):
    """Register the IPython extension."""
    ipython.register_magics(WebHDFSMagics)
