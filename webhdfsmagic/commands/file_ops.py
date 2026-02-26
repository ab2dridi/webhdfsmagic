"""
File operation commands for WebHDFS.

This module implements commands for reading, downloading, and uploading files:
- cat: Display file content (with smart formatting for CSV/Parquet)
- get: Download files from HDFS (with parallel download support)
- put: Upload files to HDFS (with parallel upload support)
"""

import fnmatch
import glob
import io
import os
import re
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests
from tabulate import tabulate

from .base import BaseCommand


class CatCommand(BaseCommand):
    """Display file content from HDFS with smart formatting for structured files."""

    def execute(
        self,
        file_path: str,
        num_lines: int = 100,
        allow_redirects: bool = False,
        format_type: Optional[str] = None,
        raw: bool = False,
    ) -> str:
        """
        Read and display file content with intelligent formatting.

        Args:
            file_path: HDFS file path
            num_lines: Number of lines to display (-1 for all)
            allow_redirects: Whether to follow redirects automatically
            format_type: Force specific format ('csv', 'parquet', 'pandas', 'raw')
            raw: If True, display raw content without formatting

        Returns:
            File content as string or formatted table

        Raises:
            Exception: On HTTP or processing errors
        """
        try:
            # Detect file type early for special handling
            file_type_hint = "parquet" if file_path.lower().endswith(".parquet") else None

            # For Parquet files, check file size first (they must be fully downloaded)
            if file_type_hint == "parquet" and num_lines != -1:
                file_size = self._get_file_size(file_path)
                size_mb = file_size / (1024 * 1024)

                # Warn if Parquet file is large (> 100 MB)
                if size_mb > 100:
                    warning = (
                        f"⚠️  Large Parquet file detected: {size_mb:.1f} MB\n"
                        f"   Parquet files must be fully downloaded for preview.\n"
                        f"   Consider using: %hdfs get {file_path} . (download first)\n\n"
                    )
                    # Still proceed but with warning
                    return warning + "Use -n -1 to force full download."

            # Calculate max bytes to fetch based on num_lines and file type
            # For Parquet: no limit (binary format requires full file)
            # For text/CSV: limit to 50MB for partial reads
            if file_type_hint == "parquet":
                max_bytes = None  # Parquet needs full file
            else:
                max_bytes = None if num_lines == -1 else 50 * 1024 * 1024  # 50 MB

            # Fetch file content
            content = self._fetch_content(file_path, allow_redirects, max_bytes)

            # If raw mode requested, return raw content
            if raw or format_type == "raw":
                return self._format_raw_content(content, num_lines)

            # Detect file type and apply smart formatting
            file_type = self._detect_file_type(file_path, content)

            if file_type == "csv":
                return self._format_csv(content, num_lines, format_type)
            elif file_type == "parquet":
                return self._format_parquet(content, num_lines, format_type)
            else:
                # Default to raw display for non-structured files
                return self._format_raw_content(content, num_lines)

        except Exception as e:
            tb = traceback.format_exc()
            return f"Error: {str(e)}\nTraceback:\n{tb}"

    def _get_file_size(self, file_path: str) -> int:
        """
        Get file size from HDFS using GETFILESTATUS operation.

        Args:
            file_path: HDFS file path

        Returns:
            File size in bytes
        """
        url = (
            f"{self.client.knox_url}{self.client.webhdfs_api}"
            f"{file_path}?op=GETFILESTATUS&user.name={self.client.auth_user}"
        )

        response = requests.get(
            url,
            auth=(self.client.auth_user, self.client.auth_password),
            verify=self.client.verify_ssl,
        )
        response.raise_for_status()

        file_status = response.json()
        return file_status["FileStatus"]["length"]

    def _fetch_content(
        self, file_path: str, allow_redirects: bool, max_bytes: Optional[int] = None
    ) -> bytes:
        """
        Fetch raw file content from HDFS.

        Args:
            file_path: HDFS file path
            allow_redirects: Whether to follow redirects automatically
            max_bytes: Maximum number of bytes to read (None for entire file)

        Returns:
            File content as bytes
        """
        # Build URL with optional length parameter
        url = f"{self.client.knox_url}{self.client.webhdfs_api}{file_path}?op=OPEN"

        # Add length parameter to limit bytes read from HDFS
        if max_bytes is not None:
            url += f"&length={max_bytes}"

        response = requests.get(
            url,
            auth=(self.client.auth_user, self.client.auth_password),
            verify=self.client.verify_ssl,
            allow_redirects=allow_redirects,
        )

        # Handle redirect manually to fix DataNode hostname
        if response.status_code == 307:
            response = self._handle_redirect(response)

        response.raise_for_status()
        return response.content

    def _detect_file_type(self, file_path: str, content: bytes) -> str:
        """Detect file type from extension and content."""
        file_path_lower = file_path.lower()

        # Check file extension
        if file_path_lower.endswith(".csv"):
            return "csv"
        elif file_path_lower.endswith((".tsv", ".tab")):
            return "csv"  # TSV is handled as CSV with different delimiter
        elif file_path_lower.endswith(".parquet"):
            return "parquet"
        elif file_path_lower.endswith(".json"):
            return "json"

        # Try to detect from content (first few bytes)
        try:
            # Check for Parquet magic number
            if content[:4] == b"PAR1":
                return "parquet"

            # Try to decode as text and check for CSV patterns
            text_sample = content[:1000].decode("utf-8", errors="ignore")
            if "," in text_sample or "\t" in text_sample:
                # Count delimiters in first line
                first_line = text_sample.split("\n")[0] if "\n" in text_sample else text_sample
                if first_line.count(",") >= 1 or first_line.count("\t") >= 1:
                    return "csv"
        except Exception:
            pass

        return "text"

    def _format_raw_content(self, content: bytes, num_lines: int) -> str:
        """Format content as raw text."""
        text = content.decode("utf-8", errors="replace")
        lines = text.splitlines()

        if num_lines == -1:
            return "\n".join(lines)
        else:
            return "\n".join(lines[:num_lines])

    def _format_csv(self, content: bytes, num_lines: int, format_type: Optional[str]) -> str:
        """Format CSV content as a table."""
        try:
            # Decode content
            text = content.decode("utf-8", errors="replace")

            # Try to infer delimiter
            delimiter = self._infer_delimiter(text)

            # Parse CSV into DataFrame
            df = pd.read_csv(
                io.StringIO(text), sep=delimiter, on_bad_lines="skip", encoding_errors="replace"
            )

            # Limit rows if requested
            if num_lines != -1 and len(df) > num_lines:
                df = df.head(num_lines)
                truncated = True
            else:
                truncated = False

            # Return as Polars DataFrame if requested (convert from pandas)
            if format_type == "polars":
                import polars as pl

                df_polars = pl.from_pandas(df)
                result = str(df_polars)
                if truncated:
                    total_lines = len(content.decode("utf-8", errors="replace").splitlines())
                    result += f"\n\n... (showing first {num_lines} of {total_lines} rows)"
                return result

            # Return as pandas DataFrame if requested
            if format_type == "pandas":
                return str(df)

            # Format as table
            table = tabulate(df, headers="keys", tablefmt="grid", showindex=False, maxcolwidths=50)

            # Add truncation notice if needed
            if truncated:
                total_lines = len(content.decode("utf-8", errors="replace").splitlines())
                table += f"\n\n... (showing first {num_lines} of {total_lines} rows)"

            return table

        except Exception as e:
            # Fallback to raw display if parsing fails
            error_msg = f"[CSV parsing failed: {str(e)}]\n\n"
            return error_msg + self._format_raw_content(content, num_lines)

    def _format_parquet(self, content: bytes, num_lines: int, format_type: Optional[str]) -> str:
        """Format Parquet content as a table using Polars for efficiency."""
        try:
            import polars as pl

            # Read Parquet file from bytes using Polars (highly efficient!)
            df = pl.read_parquet(io.BytesIO(content))

            # Limit rows if requested (Polars handles this efficiently)
            if num_lines != -1 and len(df) > num_lines:
                df = df.head(num_lines)
                truncated = True
            else:
                truncated = False

            # Return as Polars DataFrame if requested (shows schema and types)
            if format_type == "polars":
                result = str(df)
                if truncated:
                    result += f"\n\n... (showing first {num_lines} rows)"
                return result

            # Convert to pandas for display formatting
            df_pandas = df.to_pandas()

            # Return as pandas DataFrame if requested
            if format_type == "pandas":
                return str(df_pandas)

            # Format as table
            table = tabulate(
                df_pandas, headers="keys", tablefmt="grid", showindex=False, maxcolwidths=50
            )

            # Add truncation notice if needed
            if truncated:
                table += f"\n\n... (showing first {num_lines} of {len(df)} rows)"

            return table

        except Exception as e:
            # Fallback error message
            error_msg = f"[Parquet parsing failed: {str(e)}]\n\n"
            return error_msg + "Raw content not available for binary files."

    def _infer_delimiter(self, text: str) -> str:
        """Infer the delimiter used in CSV file."""
        # Get first few lines as sample
        lines = text.split("\n")[:5]

        # Count common delimiters
        delimiters = [",", "\t", ";", "|"]
        counts = {}

        for delimiter in delimiters:
            # Count occurrences in each line
            line_counts = [line.count(delimiter) for line in lines if line.strip()]
            if line_counts:
                # Check if delimiter appears consistently
                if len(set(line_counts)) == 1 and line_counts[0] > 0:
                    counts[delimiter] = line_counts[0]

        # Return delimiter with highest count
        if counts:
            return max(counts, key=counts.get)

        # Default to comma
        return ","

    def _handle_redirect(self, response: requests.Response) -> requests.Response:
        """Handle HTTP 307 redirect and fix Docker internal hostnames."""
        redirect_url = response.headers.get("Location")
        parsed = urlparse(redirect_url)

        # Fix Docker internal hostnames (12-char hex) -> localhost
        hostname = parsed.hostname
        if re.match(r"^[0-9a-f]{12}$", hostname):
            hostname = "localhost"

        # Ensure user.name is in the query parameters
        query_params = parse_qs(parsed.query)
        if "user.name" not in query_params and self.client.auth_user:
            query_params["user.name"] = [self.client.auth_user]

        # Reconstruct URL
        fixed_url = urlunparse(
            (
                parsed.scheme,
                f"{hostname}:{parsed.port}" if parsed.port else hostname,
                parsed.path,
                parsed.params,
                urlencode(query_params, doseq=True),
                parsed.fragment,
            )
        )

        return requests.get(
            fixed_url,
            auth=(self.client.auth_user, self.client.auth_password),
            verify=self.client.verify_ssl,
        )


class GetCommand(BaseCommand):
    """Download files from HDFS to local filesystem."""

    def execute(
        self, hdfs_source: str, local_dest: str, format_ls_func: callable, threads: int = 1
    ) -> str:
        """
        Download file(s) from HDFS.

        Args:
            hdfs_source: HDFS path or pattern (supports wildcards)
            local_dest: Local destination path
            format_ls_func: Function to list HDFS directory
            threads: Number of parallel threads (default: 1)

        Returns:
            Success/error message(s)
        """
        # Expand ~ in local_dest to handle home directory
        local_dest_expanded = os.path.expanduser(local_dest)

        # Handle wildcards
        if "*" in hdfs_source or "?" in hdfs_source:
            return self._download_multiple(
                hdfs_source, local_dest, local_dest_expanded, format_ls_func, threads
            )
        else:
            return self._download_single(hdfs_source, local_dest, local_dest_expanded)

    def _download_multiple(
        self,
        hdfs_pattern: str,
        local_dest: str,
        local_dest_expanded: str,
        format_ls_func: callable,
        threads: int = 1,
    ) -> str:
        """Download multiple files matching pattern, optionally in parallel."""
        base_dir = os.path.dirname(hdfs_pattern)
        pattern = os.path.basename(hdfs_pattern)

        df = format_ls_func(base_dir)
        matching_files = df[df["name"].apply(lambda x: fnmatch.fnmatch(x, pattern))]

        if matching_files.empty:
            return f"No file matches the pattern {hdfs_pattern}"

        # Prepare list of download tasks
        download_tasks = []
        for _, row in matching_files.iterrows():
            file_name = row["name"]
            hdfs_file = base_dir.rstrip("/") + "/" + file_name
            final_local_dest = self._resolve_local_path(local_dest, local_dest_expanded, file_name)
            # Ensure parent directory exists
            parent_dir = os.path.dirname(final_local_dest)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            download_tasks.append((hdfs_file, final_local_dest, file_name))

        # Download files in parallel if threads > 1
        if threads > 1:
            return self._download_multiple_parallel(download_tasks, threads)
        else:
            return self._download_multiple_sequential(download_tasks)

    def _download_multiple_sequential(self, download_tasks: list) -> str:
        """Download files sequentially."""
        responses = []
        for hdfs_file, final_local_dest, file_name in download_tasks:
            try:
                self._download_file(hdfs_file, final_local_dest)
                responses.append(f"{file_name} downloaded to {final_local_dest}")
            except Exception as e:
                tb = traceback.format_exc()
                responses.append(f"Error: {str(e)}\nTraceback:\n{tb}")
        if responses:
            return "\n" + "\n".join(responses) + "\n"
        return ""

    def _download_multiple_parallel(self, download_tasks: list, threads: int) -> str:
        """Download files in parallel using ThreadPoolExecutor."""
        responses = []
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_task = {
                executor.submit(self._download_file, hdfs_file, final_local_dest): (
                    file_name,
                    final_local_dest,
                    hdfs_file,
                )
                for hdfs_file, final_local_dest, file_name in download_tasks
            }

            for future in as_completed(future_to_task):
                file_name, final_local_dest, hdfs_file = future_to_task[future]
                try:
                    future.result()
                    responses.append(f"{file_name} downloaded to {final_local_dest}")
                except Exception as e:
                    tb = traceback.format_exc()
                    responses.append(f"Error downloading {file_name}: {str(e)}\nTraceback:\n{tb}")
        if responses:
            return "\n" + "\n".join(responses) + "\n"
        return ""

    def _download_single(self, hdfs_source: str, local_dest: str, local_dest_expanded: str) -> str:
        """Download single file."""
        try:
            # Expand ~ in local_dest to handle home directory
            final_local_dest = local_dest_expanded

            if local_dest == ".":
                final_local_dest = os.path.join(os.getcwd(), os.path.basename(hdfs_source))
            elif local_dest in ["~", "~/"]:
                final_local_dest = os.path.join(
                    os.path.expanduser("~"), os.path.basename(hdfs_source)
                )
            else:
                if local_dest_expanded.endswith("/") or local_dest_expanded.endswith("."):
                    if not local_dest_expanded.endswith("/"):
                        local_dest_expanded += "/"
                    basename = os.path.basename(hdfs_source)
                    final_local_dest = local_dest_expanded + basename

            # Ensure parent directory exists
            parent_dir = os.path.dirname(final_local_dest)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            self._download_file(hdfs_source, final_local_dest)
            return f"{hdfs_source} downloaded to {final_local_dest}"
        except Exception as e:
            tb = traceback.format_exc()
            return f"Error: {str(e)}\nTraceback:\n{tb}"

    def _resolve_local_path(self, local_dest: str, local_dest_expanded: str, file_name: str) -> str:
        """Resolve final local destination path for wildcard downloads."""
        if local_dest in [".", "~", "~/"]:
            base = os.getcwd() if local_dest == "." else os.path.expanduser("~")
            return os.path.join(base, file_name)
        elif local_dest_expanded.endswith("/."):
            # Handle paths like /test_webhdfs/.
            base = local_dest_expanded[:-2]  # Remove the trailing /.
            return os.path.join(base, file_name)
        elif local_dest_expanded.endswith("/") or local_dest_expanded.endswith("."):
            # Handle directory paths
            ends_with_dot = local_dest_expanded.endswith(".")
            ends_with_slash_dot = local_dest_expanded.endswith("/.")
            if ends_with_dot and not ends_with_slash_dot:
                local_dest_expanded += "/"
            final_path = os.path.join(local_dest_expanded, file_name)
        else:
            # Simple path
            final_path = os.path.join(local_dest_expanded, file_name)

        # Normalize the path to remove any redundant ./ or //
        return os.path.normpath(final_path)

    def _download_file(self, hdfs_path: str, local_path: str):
        """Download a single file via streaming."""
        response = requests.get(
            f"{self.client.knox_url}{self.client.webhdfs_api}{hdfs_path}?op=OPEN",
            auth=(self.client.auth_user, self.client.auth_password),
            verify=self.client.verify_ssl,
            stream=True,
            allow_redirects=False,
        )

        # Handle redirect manually to fix DataNode hostname
        if response.status_code == 307:
            response = self._handle_redirect(response)

        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    def _handle_redirect(self, response: requests.Response) -> requests.Response:
        """Handle HTTP 307 redirect and fix Docker internal hostnames."""
        redirect_url = response.headers.get("Location")
        parsed = urlparse(redirect_url)

        # Fix Docker internal hostnames
        hostname = parsed.hostname
        if re.match(r"^[0-9a-f]{12}$", hostname):
            hostname = "localhost"

        # Ensure user.name is in the query parameters
        query_params = parse_qs(parsed.query)
        if "user.name" not in query_params and self.client.auth_user:
            query_params["user.name"] = [self.client.auth_user]

        # Reconstruct URL
        fixed_url = urlunparse(
            (
                parsed.scheme,
                f"{hostname}:{parsed.port}" if parsed.port else hostname,
                parsed.path,
                parsed.params,
                urlencode(query_params, doseq=True),
                parsed.fragment,
            )
        )

        return requests.get(
            fixed_url,
            auth=(self.client.auth_user, self.client.auth_password),
            verify=self.client.verify_ssl,
            stream=True,
        )


class PutCommand(BaseCommand):
    """Upload files to HDFS."""

    def _fix_docker_hostname(self, url: str) -> str:
        """Fix Docker internal hostnames in redirect URLs."""
        import re
        from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

        parsed = urlparse(url)
        hostname = parsed.hostname

        # Fix Docker internal hostnames (12-character hex IDs)
        if hostname and re.match(r"^[0-9a-f]{12}$", hostname):
            hostname = "localhost"

        # Ensure user.name is in the query parameters
        query_params = parse_qs(parsed.query)
        if "user.name" not in query_params and self.client.auth_user:
            query_params["user.name"] = [self.client.auth_user]

        # Reconstruct URL
        netloc = f"{hostname}:{parsed.port}" if parsed.port else hostname
        query_string = urlencode(query_params, doseq=True)

        return urlunparse(
            (parsed.scheme, netloc, parsed.path, parsed.params, query_string, parsed.fragment)
        )

    def execute(self, local_pattern: str, hdfs_dest: str, threads: int = 1) -> str:
        """
        Upload file(s) to HDFS.

        Args:
            local_pattern: Local file path or pattern (supports wildcards)
            hdfs_dest: HDFS destination path
            threads: Number of parallel threads (default: 1)

        Returns:
            Success/error message(s)

        Example:
            >>> cmd = PutCommand(client)
            >>> result = cmd.execute("data/*.csv", "/hdfs/data/", threads=4)
            'file1.csv uploaded to /hdfs/data/\\nfile2.csv uploaded to /hdfs/data/'
        """
        local_files = glob.glob(os.path.expanduser(local_pattern))
        if not local_files:
            return f"No local files match the pattern: {local_pattern}"

        # Upload files in parallel if threads > 1
        if threads > 1:
            return self._upload_multiple_parallel(local_files, hdfs_dest, threads)
        else:
            return self._upload_multiple_sequential(local_files, hdfs_dest)

    def _upload_multiple_sequential(self, local_files: list, hdfs_dest: str) -> str:
        """Upload files sequentially."""
        responses = []
        for local_file in local_files:
            response = self._upload_single_file(local_file, hdfs_dest)
            responses.append(response)
        if responses:
            return "\n" + "\n".join(responses) + "\n"
        return ""

    def _upload_multiple_parallel(self, local_files: list, hdfs_dest: str, threads: int) -> str:
        """Upload files in parallel using ThreadPoolExecutor."""
        responses = []
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_file = {
                executor.submit(self._upload_single_file, local_file, hdfs_dest): local_file
                for local_file in local_files
            }

            for future in as_completed(future_to_file):
                local_file = future_to_file[future]
                try:
                    response = future.result()
                    responses.append(response)
                except Exception as e:
                    responses.append(f"Error uploading {local_file}: {str(e)}")

        if responses:
            return "\n" + "\n".join(responses) + "\n"
        return ""

    def _upload_single_file(self, local_file: str, hdfs_dest: str) -> str:
        """Upload a single file to HDFS."""
        try:
            # Phase 1: Create file
            init_url = f"{self.client.knox_url}{self.client.webhdfs_api}{hdfs_dest}"
            if hdfs_dest.endswith("/") or hdfs_dest.endswith("."):
                basename = os.path.basename(local_file)
                init_url = f"{self.client.knox_url}{self.client.webhdfs_api}{hdfs_dest}{basename}"

            init_response = requests.put(
                init_url,
                params={"op": "CREATE", "overwrite": "true"},
                auth=(self.client.auth_user, self.client.auth_password),
                verify=self.client.verify_ssl,
                allow_redirects=False,
            )

            if init_response.status_code == 307:
                upload_url = init_response.headers["Location"]

                # Fix Docker internal hostnames
                upload_url = self._fix_docker_hostname(upload_url)

                with open(local_file, "rb") as f:
                    upload_response = requests.put(
                        upload_url,
                        data=f,
                        auth=(self.client.auth_user, self.client.auth_password),
                        verify=self.client.verify_ssl,
                    )
                if upload_response.status_code in [200, 201]:
                    return f"{local_file} uploaded to {hdfs_dest}"
                else:
                    return f"Upload failed for {local_file}, status: {upload_response.status_code}"
            else:
                return f"Initiation failed for {local_file}, status: {init_response.status_code}"
        except Exception as e:
            tb = traceback.format_exc()
            return f"Error uploading {local_file}: {str(e)}\nTraceback:\n{tb}"
