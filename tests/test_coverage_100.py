"""
Tests pour atteindre 100% de couverture sur les modules critiques.
"""

import os
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest


class TestClientExceptionHandling:
    """Tests pour client.py - gestion des exceptions génériques"""

    @patch("webhdfsmagic.client.requests.request")
    def test_execute_generic_exception(self, mock_request):
        """Test la gestion d'une exception générique (non-HTTPError)"""
        from webhdfsmagic.client import WebHDFSClient

        # Créer un client avec les bons paramètres
        client = WebHDFSClient(
            knox_url="http://localhost:8443/gateway/default",
            webhdfs_api="/webhdfs/v1",
            auth_user="testuser",
            auth_password="testpass"
        )

        # Simuler une exception générique (pas HTTPError)
        mock_request.side_effect = ValueError("Connection error")

        with pytest.raises(ValueError):
            client.execute("GET", "LISTSTATUS", "/test")


class TestFileOpsEdgeCases:
    """Tests pour commands/file_ops.py - cas limites"""

    def test_get_no_matching_files(self):
        """Test get avec aucun fichier correspondant au pattern (ligne 151)"""
        from webhdfsmagic.commands.file_ops import GetCommand

        mock_client = Mock()

        # Simuler format_ls_func qui retourne un DataFrame vide ou sans correspondance
        def mock_format_ls(path):
            return pd.DataFrame([
                {"name": "other.txt", "type": "FILE"},
                {"name": "another.csv", "type": "FILE"}
            ])

        command = GetCommand(mock_client)
        result = command.execute("/path/test*.log", "/tmp/dest", mock_format_ls)

        assert "No file matches the pattern" in result

    def test_get_destination_ends_with_dot(self):
        """Test get avec destination se terminant par '.' (ligne 233)"""
        from webhdfsmagic.commands.file_ops import GetCommand

        mock_client = Mock()
        mock_client.knox_url = "http://localhost:8443/gateway/default"
        mock_client.webhdfs_api = "/webhdfs/v1"
        mock_client.auth_user = "testuser"
        mock_client.auth_password = "testpass"
        mock_client.verify_ssl = False

        # Mock pour le téléchargement avec wildcard pour tester _download_multiple
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.iter_content = lambda chunk_size: [b"test data"]

        # Mock format_ls_func qui retourne des fichiers correspondants
        def mock_format_ls(path):
            return pd.DataFrame([
                {"name": "file1.txt", "type": "FILE"},
                {"name": "file2.txt", "type": "FILE"}
            ])

        with patch("requests.get", return_value=mock_response):
            with tempfile.TemporaryDirectory() as tmpdir:
                # Destination se termine par "." (pas "/.")
                dest_path = os.path.join(tmpdir, "dest.")

                command = GetCommand(mock_client)
                # Utiliser un wildcard pour forcer _download_multiple
                # qui appelle _resolve_local_path
                command.execute("/remote/*.txt", dest_path, mock_format_ls)

                # Les fichiers devraient être téléchargés dans dest./
                expected_file1 = os.path.join(dest_path, "file1.txt")
                expected_file2 = os.path.join(dest_path, "file2.txt")
                assert os.path.exists(expected_file1) or os.path.exists(expected_file2)

    def test_get_docker_hostname_fix(self):
        """Test get avec hostname Docker 12 caractères hexa (ligne 270)"""
        from webhdfsmagic.commands.file_ops import GetCommand

        mock_client = Mock()
        mock_client.knox_url = "http://localhost:8443/gateway/default"
        mock_client.webhdfs_api = "/webhdfs/v1"
        mock_client.auth_user = "testuser"
        mock_client.auth_password = "testpass"
        mock_client.verify_ssl = False

        docker_hostname = "a1b2c3d4e5f6"  # 12 caractères hexa

        # Premier appel: redirect avec hostname Docker
        mock_redirect = Mock()
        mock_redirect.status_code = 307
        mock_redirect.headers = {"Location": f"http://{docker_hostname}:9864/webhdfs/v1/file.txt?op=OPEN"}

        # Deuxième appel: contenu réel
        mock_data = Mock()
        mock_data.status_code = 200
        mock_data.iter_content = lambda chunk_size: [b"test data"]

        with patch("requests.get", side_effect=[mock_redirect, mock_data]) as mock_get:
            with tempfile.TemporaryDirectory() as tmpdir:
                dest = os.path.join(tmpdir, "output.txt")

                command = GetCommand(mock_client)
                command.execute("/remote/file.txt", dest, lambda x: pd.DataFrame())

                # Vérifier que le deuxième appel utilise localhost au lieu du hostname Docker
                second_call = mock_get.call_args_list[1][0][0]
                assert "localhost" in second_call
                assert docker_hostname not in second_call

    def test_put_docker_hostname_fix(self):
        """Test put avec hostname Docker 12 caractères hexa (ligne 308)"""
        from webhdfsmagic.commands.file_ops import PutCommand

        mock_client = Mock()
        mock_client.auth_user = "testuser"

        command = PutCommand(mock_client)

        # Test direct de _fix_docker_hostname avec un hostname Docker
        docker_url = "http://fedcba987654:9864/webhdfs/v1/test.txt?op=CREATE"
        fixed_url = command._fix_docker_hostname(docker_url)

        # Doit remplacer le hostname par localhost
        assert "localhost:9864" in fixed_url
        assert "fedcba987654" not in fixed_url

    def test_put_docker_hostname_fix_integration(self):
        """Test put avec hostname Docker via execute (intégration)"""
        from webhdfsmagic.commands.file_ops import PutCommand

        mock_client = Mock()
        mock_client.knox_url = "http://localhost:8443/gateway/default"
        mock_client.webhdfs_api = "/webhdfs/v1"
        mock_client.auth_user = "testuser"
        mock_client.auth_password = "testpass"
        mock_client.verify_ssl = False

        docker_hostname = "a1b2c3d4e5f6"  # 12 caractères hexa

        # Mock pour requests.put (CREATE)
        mock_init = Mock()
        mock_init.status_code = 307
        mock_init.headers = {"Location": f"http://{docker_hostname}:9864/webhdfs/v1/test.txt?op=CREATE"}

        # Mock pour l'upload
        mock_upload = Mock()
        mock_upload.status_code = 201

        with patch("requests.put", side_effect=[mock_init, mock_upload]) as mock_put:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write("test content")
                temp_file = f.name

            try:
                command = PutCommand(mock_client)
                command.execute(temp_file, "/remote/test.txt")

                # Vérifier que le deuxième appel (upload) utilise localhost
                if len(mock_put.call_args_list) >= 2:
                    upload_url = mock_put.call_args_list[1][0][0]
                    assert "localhost" in upload_url
                    assert docker_hostname not in upload_url
            finally:
                os.unlink(temp_file)

    def test_put_upload_failure(self):
        """Test put avec échec d'upload (lignes 385-387)"""
        from webhdfsmagic.commands.file_ops import PutCommand

        mock_client = Mock()
        mock_client.knox_url = "http://localhost:8443/gateway/default"
        mock_client.webhdfs_api = "/webhdfs/v1"
        mock_client.auth_user = "testuser"
        mock_client.auth_password = "testpass"
        mock_client.verify_ssl = False

        # Premier appel: CREATE avec redirect
        mock_create = Mock()
        mock_create.status_code = 307
        mock_create.headers = {"Location": "http://localhost:9864/webhdfs/v1/test.txt?op=CREATE"}

        # Deuxième appel: upload échoue avec code d'erreur
        mock_upload_fail = Mock()
        mock_upload_fail.status_code = 500  # Erreur serveur

        with patch("requests.put", side_effect=[mock_create, mock_upload_fail]):
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write("test content")
                temp_file = f.name

            try:
                command = PutCommand(mock_client)
                result = command.execute(temp_file, "/remote/test.txt")

                # Devrait contenir un message d'échec d'upload
                assert "Upload failed" in result
                assert "500" in result
            finally:
                os.unlink(temp_file)

    def test_put_initiation_failure(self):
        """Test put avec échec d'initiation (lignes 389-392)"""
        from webhdfsmagic.commands.file_ops import PutCommand

        mock_client = Mock()
        mock_client.knox_url = "http://localhost:8443/gateway/default"
        mock_client.webhdfs_api = "/webhdfs/v1"
        mock_client.auth_user = "testuser"
        mock_client.auth_password = "testpass"
        mock_client.verify_ssl = False

        # Premier appel: CREATE échoue (pas de redirect 307)
        mock_create_fail = Mock()
        mock_create_fail.status_code = 403  # Forbidden

        with patch("requests.put", return_value=mock_create_fail):
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write("test content")
                temp_file = f.name

            try:
                command = PutCommand(mock_client)
                result = command.execute(temp_file, "/remote/test.txt")

                # Devrait contenir un message d'échec d'initiation
                assert "Initiation failed" in result
                assert "403" in result
            finally:
                os.unlink(temp_file)

    def test_put_exception_handling(self):
        """Test put avec exception (lignes 393-394)"""
        from webhdfsmagic.commands.file_ops import PutCommand

        mock_client = Mock()
        mock_client.knox_url = "http://localhost:8443/gateway/default"
        mock_client.webhdfs_api = "/webhdfs/v1"
        mock_client.auth_user = "testuser"
        mock_client.auth_password = "testpass"
        mock_client.verify_ssl = False

        # Simuler une exception lors de l'upload
        with patch("requests.put", side_effect=Exception("Network timeout")):
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write("test content")
                temp_file = f.name

            try:
                command = PutCommand(mock_client)
                result = command.execute(temp_file, "/remote/test.txt")

                # Devrait contenir un message d'erreur
                assert "Error for" in result
                assert "Network timeout" in result
            finally:
                os.unlink(temp_file)


class TestLoggerEdgeCases:
    """Tests pour logger.py - cas limites"""

    def test_logger_duplicate_handlers(self):
        """Test que les handlers ne sont pas dupliqués"""
        from webhdfsmagic.logger import WebHDFSLogger

        # Réinitialiser le singleton
        WebHDFSLogger._instance = None

        logger1 = WebHDFSLogger()
        initial_handler_count = len(logger1._logger.handlers)

        # Appeler _initialize_logger à nouveau
        logger1._initialize_logger()

        # Le nombre de handlers ne devrait pas avoir changé
        assert len(logger1._logger.handlers) == initial_handler_count

    def test_log_http_request_with_auth(self):
        """Test log_http_request avec paramètre auth"""
        from webhdfsmagic.logger import WebHDFSLogger

        logger = WebHDFSLogger()

        # Ne devrait pas lever d'exception
        logger.log_http_request(
            method="GET",
            url="http://localhost:9870/webhdfs/v1/test",
            auth=("username", "password")
        )


class TestBaseCommandNotImplemented:
    """Tests pour commands/base.py - ligne 41"""

    def test_base_command_execute_not_implemented(self):
        """Test que BaseCommand.execute() lève NotImplementedError (ligne 41)"""
        from webhdfsmagic.commands.base import BaseCommand

        # Créer une sous-classe concrète qui appelle super().execute()
        class TestCommand(BaseCommand):
            def execute(self, *args, **kwargs):
                # Appeler la méthode parent pour atteindre ligne 41
                return super().execute(*args, **kwargs)

        mock_client = Mock()
        command = TestCommand(mock_client)

        with pytest.raises(NotImplementedError, match="Subclasses must implement execute"):
            command.execute()
