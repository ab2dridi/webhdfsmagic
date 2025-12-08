"""
WebHDFS/Knox HTTP client.

This module provides the HTTP client for interacting with the WebHDFS REST API
through Apache Knox gateway.
"""

from typing import Any, Optional, Union

import requests

from .logger import get_logger


class WebHDFSClient:
    """HTTP client for WebHDFS operations through Knox gateway."""

    def __init__(
        self,
        knox_url: str,
        webhdfs_api: str,
        auth_user: str,
        auth_password: str,
        verify_ssl: Union[bool, str] = False,
    ):
        """
        Initialize WebHDFS client.

        Args:
            knox_url: Knox gateway base URL
            webhdfs_api: WebHDFS API path (usually "/webhdfs/v1")
            auth_user: Authentication username
            auth_password: Authentication password
            verify_ssl: SSL verification (bool or path to certificate)
        """
        self.knox_url = knox_url
        self.webhdfs_api = webhdfs_api
        self.auth_user = auth_user
        self.auth_password = auth_password
        self.verify_ssl = verify_ssl
        self.logger = get_logger()

        # Log client initialization
        self.logger.info(
            f"WebHDFSClient initialized: knox_url={knox_url}, "
            f"webhdfs_api={webhdfs_api}, user={auth_user}, verify_ssl={verify_ssl}"
        )

    def execute(
        self,
        method: str,
        operation: str,
        path: str,
        stream: bool = False,
        allow_redirects: bool = True,
        **params,
    ) -> Union[requests.Response, dict[str, Any]]:
        """
        Execute a WebHDFS request.

        Args:
            method: HTTP method (GET, PUT, DELETE, POST)
            operation: WebHDFS operation (LISTSTATUS, MKDIRS, etc.)
            path: HDFS path
            stream: If True, return Response object for streaming
            allow_redirects: Whether to follow redirects
            **params: Additional query parameters

        Returns:
            Response object (if stream=True) or decoded JSON dict

        Raises:
            requests.HTTPError: On HTTP errors
        """
        url = f"{self.knox_url}{self.webhdfs_api}{path}"
        params["op"] = operation

        # Add user.name if not present and user is configured
        if "user.name" not in params and self.auth_user:
            params["user.name"] = self.auth_user

        # Log request
        self.logger.log_http_request(
            method=method,
            url=url,
            operation=operation,
            path=path,
            params={k: v for k, v in params.items() if k != "user.name"},
            stream=stream,
            allow_redirects=allow_redirects,
        )

        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                auth=(self.auth_user, self.auth_password),
                verify=self.verify_ssl,
                stream=stream,
                allow_redirects=allow_redirects,
            )

            # Log response
            self.logger.log_http_response(
                status_code=response.status_code,
                url=response.url,
                headers=dict(response.headers),
                stream=stream,
            )

            response.raise_for_status()

            if stream:
                return response

            return response.json() if response.content else {}

        except requests.exceptions.HTTPError as e:
            self.logger.log_error(
                operation=f"{method} {operation}",
                error=e,
                url=url,
                status_code=response.status_code if response else None,
                response_text=response.text if response else None,
            )
            raise
        except Exception as e:
            self.logger.log_error(
                operation=f"{method} {operation}",
                error=e,
                url=url,
            )
            raise

    def get(
        self, operation: str, path: str, stream: bool = False, **params
    ) -> Union[requests.Response, dict[str, Any]]:
        """Execute GET request."""
        return self.execute("GET", operation, path, stream=stream, **params)

    def put(
        self, operation: str, path: str, data: Optional[bytes] = None, **params
    ) -> dict[str, Any]:
        """
        Execute PUT request.

        Args:
            operation: WebHDFS operation
            path: HDFS path
            data: Optional data payload
            **params: Additional query parameters

        Returns:
            Decoded JSON response
        """
        url = f"{self.knox_url}{self.webhdfs_api}{path}"
        params["op"] = operation

        if "user.name" not in params and self.auth_user:
            params["user.name"] = self.auth_user

        response = requests.put(
            url=url,
            params=params,
            data=data,
            auth=(self.auth_user, self.auth_password),
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def post(
        self, operation: str, path: str, data: Optional[bytes] = None, **params
    ) -> dict[str, Any]:
        """Execute POST request."""
        url = f"{self.knox_url}{self.webhdfs_api}{path}"
        params["op"] = operation

        if "user.name" not in params and self.auth_user:
            params["user.name"] = self.auth_user

        response = requests.post(
            url=url,
            params=params,
            data=data,
            auth=(self.auth_user, self.auth_password),
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def delete(self, operation: str, path: str, **params) -> dict[str, Any]:
        """Execute DELETE request."""
        return self.execute("DELETE", operation, path, **params)
