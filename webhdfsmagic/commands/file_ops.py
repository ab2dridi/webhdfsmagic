"""
File operation commands for WebHDFS.

This module implements commands for reading, downloading, and uploading files:
- cat: Display file content
- get: Download files from HDFS
- put: Upload files to HDFS
"""

import fnmatch
import glob
import os
import re
import traceback
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

from .base import BaseCommand


class CatCommand(BaseCommand):
    """Display file content from HDFS."""

    def execute(
        self,
        file_path: str,
        num_lines: int = 100,
        allow_redirects: bool = False
    ) -> str:
        """
        Read and display file content.

        Args:
            file_path: HDFS file path
            num_lines: Number of lines to display (-1 for all)
            allow_redirects: Whether to follow redirects automatically

        Returns:
            File content as string

        Raises:
            Exception: On HTTP or processing errors
        """
        try:
            url = (
                f"{self.client.knox_url}{self.client.webhdfs_api}"
                f"{file_path}?op=OPEN"
            )
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
            content = response.content.decode("utf-8", errors="replace")
            lines = content.splitlines()

            if num_lines == -1:
                return "\n".join(lines)
            else:
                return "\n".join(lines[:num_lines])
        except Exception as e:
            tb = traceback.format_exc()
            return f"Error: {str(e)}\nTraceback:\n{tb}"

    def _handle_redirect(self, response: requests.Response) -> requests.Response:
        """Handle HTTP 307 redirect and fix Docker internal hostnames."""
        redirect_url = response.headers.get("Location")
        parsed = urlparse(redirect_url)

        # Fix Docker internal hostnames (12-char hex) -> localhost
        hostname = parsed.hostname
        if re.match(r'^[0-9a-f]{12}$', hostname):
            hostname = 'localhost'

        # Ensure user.name is in the query parameters
        query_params = parse_qs(parsed.query)
        if 'user.name' not in query_params and self.client.auth_user:
            query_params['user.name'] = [self.client.auth_user]

        # Reconstruct URL
        fixed_url = urlunparse((
            parsed.scheme,
            f'{hostname}:{parsed.port}' if parsed.port else hostname,
            parsed.path,
            parsed.params,
            urlencode(query_params, doseq=True),
            parsed.fragment
        ))

        return requests.get(
            fixed_url,
            auth=(self.client.auth_user, self.client.auth_password),
            verify=self.client.verify_ssl,
        )


class GetCommand(BaseCommand):
    """Download files from HDFS to local filesystem."""

    def execute(
        self,
        hdfs_source: str,
        local_dest: str,
        format_ls_func: callable
    ) -> str:
        """
        Download file(s) from HDFS.

        Args:
            hdfs_source: HDFS path or pattern (supports wildcards)
            local_dest: Local destination path
            format_ls_func: Function to list HDFS directory

        Returns:
            Success/error message(s)
        """
        # Expand ~ in local_dest to handle home directory
        local_dest_expanded = os.path.expanduser(local_dest)

        # Handle wildcards
        if "*" in hdfs_source or "?" in hdfs_source:
            return self._download_multiple(
                hdfs_source, local_dest, local_dest_expanded, format_ls_func
            )
        else:
            return self._download_single(hdfs_source, local_dest, local_dest_expanded)

    def _download_multiple(
        self,
        hdfs_pattern: str,
        local_dest: str,
        local_dest_expanded: str,
        format_ls_func: callable
    ) -> str:
        """Download multiple files matching pattern."""
        base_dir = os.path.dirname(hdfs_pattern)
        pattern = os.path.basename(hdfs_pattern)

        df = format_ls_func(base_dir)
        matching_files = df[df["name"].apply(lambda x: fnmatch.fnmatch(x, pattern))]

        if matching_files.empty:
            return f"No file matches the pattern {hdfs_pattern}"

        responses = []
        for _, row in matching_files.iterrows():
            file_name = row["name"]
            hdfs_file = base_dir.rstrip("/") + "/" + file_name

            # Determine final destination path
            final_local_dest = self._resolve_local_path(
                local_dest, local_dest_expanded, file_name
            )

            # Ensure parent directory exists
            parent_dir = os.path.dirname(final_local_dest)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            try:
                self._download_file(hdfs_file, final_local_dest)
                responses.append(f"{file_name} downloaded to {final_local_dest}")
            except Exception as e:
                tb = traceback.format_exc()
                responses.append(f"Error: {str(e)}\nTraceback:\n{tb}")

        return "\n".join(responses)

    def _download_single(
        self,
        hdfs_source: str,
        local_dest: str,
        local_dest_expanded: str
    ) -> str:
        """Download single file."""
        try:
            # Expand ~ in local_dest to handle home directory
            final_local_dest = local_dest_expanded

            if local_dest == ".":
                final_local_dest = os.path.join(
                    os.getcwd(), os.path.basename(hdfs_source)
                )
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

    def _resolve_local_path(
        self,
        local_dest: str,
        local_dest_expanded: str,
        file_name: str
    ) -> str:
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
        if re.match(r'^[0-9a-f]{12}$', hostname):
            hostname = 'localhost'

        # Ensure user.name is in the query parameters
        query_params = parse_qs(parsed.query)
        if 'user.name' not in query_params and self.client.auth_user:
            query_params['user.name'] = [self.client.auth_user]

        # Reconstruct URL
        fixed_url = urlunparse((
            parsed.scheme,
            f'{hostname}:{parsed.port}' if parsed.port else hostname,
            parsed.path,
            parsed.params,
            urlencode(query_params, doseq=True),
            parsed.fragment
        ))

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
        if hostname and re.match(r'^[0-9a-f]{12}$', hostname):
            hostname = 'localhost'

        # Ensure user.name is in the query parameters
        query_params = parse_qs(parsed.query)
        if 'user.name' not in query_params and self.client.auth_user:
            query_params['user.name'] = [self.client.auth_user]

        # Reconstruct URL
        netloc = f'{hostname}:{parsed.port}' if parsed.port else hostname
        query_string = urlencode(query_params, doseq=True)

        return urlunparse((
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            query_string,
            parsed.fragment
        ))

    def execute(self, local_pattern: str, hdfs_dest: str) -> str:
        """
        Upload file(s) to HDFS.

        Args:
            local_pattern: Local file path or pattern (supports wildcards)
            hdfs_dest: HDFS destination path

        Returns:
            Success/error message(s)

        Example:
            >>> cmd = PutCommand(client)
            >>> result = cmd.execute("data/*.csv", "/hdfs/data/")
            'file1.csv uploaded to /hdfs/data/\\nfile2.csv uploaded to /hdfs/data/'
        """
        local_files = glob.glob(os.path.expanduser(local_pattern))
        if not local_files:
            return f"No local files match the pattern: {local_pattern}"

        responses = []
        for local_file in local_files:
            try:
                # Phase 1: Create file
                init_url = f"{self.client.knox_url}{self.client.webhdfs_api}{hdfs_dest}"
                if hdfs_dest.endswith("/") or hdfs_dest.endswith("."):
                    basename = os.path.basename(local_file)
                    init_url = (
                        f"{self.client.knox_url}{self.client.webhdfs_api}"
                        f"{hdfs_dest}{basename}"
                    )

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
                        responses.append(f"{local_file} uploaded to {hdfs_dest}")
                    else:
                        responses.append(
                            f"Upload failed for {local_file}, "
                            f"status: {upload_response.status_code}"
                        )
                else:
                    responses.append(
                        f"Initiation failed for {local_file}, "
                        f"status: {init_response.status_code}"
                    )
            except Exception as e:
                responses.append(f"Error for {local_file}: {str(e)}")

        return "\n".join(responses)
