"""
Permission operation commands for WebHDFS.

This module implements commands for managing permissions and ownership:
- chmod: Change file/directory permissions
- chown: Change owner and group
"""

from typing import Optional

from .base import BaseCommand


class ChmodCommand(BaseCommand):
    """Change file/directory permissions."""

    def execute(
        self,
        path: str,
        permission: str,
        recursive: bool = False,
        format_ls_func: callable = None
    ) -> str:
        """
        Change permissions on HDFS file or directory.

        Args:
            path: HDFS path
            permission: Permission string (e.g., "755", "644")
            recursive: If True, apply recursively to all contents
            format_ls_func: Function to list directory (required for recursive)

        Returns:
            Success message

        Example:
            >>> cmd = ChmodCommand(client)
            >>> result = cmd.execute("/data/file.txt", "644")
            'Permission 644 set for /data/file.txt'
        """
        if recursive:
            if not format_ls_func:
                raise ValueError("format_ls_func required for recursive chmod")
            self._set_permission_recursive(path, permission, format_ls_func)
            return f"Recursive chmod {permission} applied on {path}"
        else:
            return self._set_permission(path, permission)

    def _set_permission(self, path: str, permission: str) -> str:
        """Set permissions for a single file or directory."""
        self.client.execute("PUT", "SETPERMISSION", path, permission=permission)
        return f"Permission {permission} set for {path}"

    def _set_permission_recursive(
        self,
        path: str,
        permission: str,
        format_ls_func: callable
    ):
        """Recursively apply permission changes."""
        self._set_permission(path, permission)
        try:
            result = format_ls_func(path)
            # Handle empty directory case
            if isinstance(result, dict) and result.get("empty_dir"):
                return
            df = result
        except Exception:
            return

        for _, row in df.iterrows():
            full_path = path.rstrip("/") + "/" + row["name"]
            if row["type"] == "DIR":
                self._set_permission_recursive(full_path, permission, format_ls_func)
            else:
                self._set_permission(full_path, permission)


class ChownCommand(BaseCommand):
    """Change file/directory owner and group."""

    def execute(
        self,
        path: str,
        owner: str,
        group: Optional[str] = None,
        recursive: bool = False,
        format_ls_func: callable = None
    ) -> str:
        """
        Change owner and group on HDFS file or directory.

        Args:
            path: HDFS path
            owner: New owner name
            group: New group name (optional)
            recursive: If True, apply recursively to all contents
            format_ls_func: Function to list directory (required for recursive)

        Returns:
            Success message

        Example:
            >>> cmd = ChownCommand(client)
            >>> result = cmd.execute("/data/file.txt", "hadoop", "supergroup")
            'Owner hadoop:supergroup set for /data/file.txt'
        """
        if recursive:
            if not format_ls_func:
                raise ValueError("format_ls_func required for recursive chown")
            self._recursive_set_owner(path, owner, group, format_ls_func)
            return f"Recursive chown {owner}:{group} applied on {path}"
        else:
            return self._set_owner(path, owner, group)

    def _set_owner(
        self,
        path: str,
        owner: str,
        group: Optional[str] = None
    ) -> str:
        """Set owner and group for a single file or directory."""
        self.client.execute("PUT", "SETOWNER", path, owner=owner, group=group)
        return f"Owner {owner}:{group} set for {path}"

    def _recursive_set_owner(
        self,
        path: str,
        owner: str,
        group: Optional[str],
        format_ls_func: callable
    ):
        """Recursively apply owner and group changes."""
        self._set_owner(path, owner, group)
        try:
            result = format_ls_func(path)
            # Handle empty directory case
            if isinstance(result, dict) and result.get("empty_dir"):
                return
            df = result
        except Exception:
            return

        for _, row in df.iterrows():
            full_path = path.rstrip("/") + "/" + row["name"]
            if row["type"] == "DIR":
                self._recursive_set_owner(full_path, owner, group, format_ls_func)
            else:
                self._set_owner(full_path, owner, group)
