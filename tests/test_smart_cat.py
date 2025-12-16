"""
Tests for smart cat functionality with CSV and Parquet formatting.
"""

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from webhdfsmagic.commands.file_ops import CatCommand


@pytest.fixture
def mock_client():
    """Create a mock WebHDFS client."""
    client = MagicMock()
    client.knox_url = "https://test.example.com/gateway/default"
    client.webhdfs_api = "/webhdfs/v1"
    client.auth_user = "testuser"
    client.auth_password = "testpass"
    client.verify_ssl = False
    return client


@pytest.fixture
def cat_command(mock_client):
    """Create a CatCommand instance with mock client."""
    return CatCommand(mock_client)


class TestCatCommandDetection:
    """Test file type detection."""

    def test_detect_csv_from_extension(self, cat_command):
        """Test CSV detection from .csv extension."""
        file_type = cat_command._detect_file_type("/data/test.csv", b"a,b,c\n1,2,3")
        assert file_type == "csv"

    def test_detect_tsv_from_extension(self, cat_command):
        """Test TSV detection from .tsv extension."""
        file_type = cat_command._detect_file_type("/data/test.tsv", b"a\tb\tc\n1\t2\t3")
        assert file_type == "csv"

    def test_detect_parquet_from_extension(self, cat_command):
        """Test Parquet detection from .parquet extension."""
        file_type = cat_command._detect_file_type("/data/test.parquet", b"PAR1...")
        assert file_type == "parquet"

    def test_detect_parquet_from_magic_number(self, cat_command):
        """Test Parquet detection from magic number."""
        file_type = cat_command._detect_file_type("/data/test.data", b"PAR1...")
        assert file_type == "parquet"

    def test_detect_csv_from_content(self, cat_command):
        """Test CSV detection from content with commas."""
        content = b"name,age,city\nJohn,30,NYC\nJane,25,LA"
        file_type = cat_command._detect_file_type("/data/test.txt", content)
        assert file_type == "csv"

    def test_detect_text_for_unknown(self, cat_command):
        """Test fallback to text for unknown file types."""
        file_type = cat_command._detect_file_type("/data/test.log", b"log message here")
        assert file_type == "text"


class TestCatCommandDelimiterInference:
    """Test delimiter inference for CSV files."""

    def test_infer_comma_delimiter(self, cat_command):
        """Test comma delimiter inference."""
        text = "a,b,c\n1,2,3\n4,5,6"
        delimiter = cat_command._infer_delimiter(text)
        assert delimiter == ","

    def test_infer_tab_delimiter(self, cat_command):
        """Test tab delimiter inference."""
        text = "a\tb\tc\n1\t2\t3\n4\t5\t6"
        delimiter = cat_command._infer_delimiter(text)
        assert delimiter == "\t"

    def test_infer_semicolon_delimiter(self, cat_command):
        """Test semicolon delimiter inference."""
        text = "a;b;c\n1;2;3\n4;5;6"
        delimiter = cat_command._infer_delimiter(text)
        assert delimiter == ";"

    def test_infer_pipe_delimiter(self, cat_command):
        """Test pipe delimiter inference."""
        text = "a|b|c\n1|2|3\n4|5|6"
        delimiter = cat_command._infer_delimiter(text)
        assert delimiter == "|"

    def test_default_to_comma_when_uncertain(self, cat_command):
        """Test default to comma when delimiter is unclear."""
        text = "plain text without delimiters"
        delimiter = cat_command._infer_delimiter(text)
        assert delimiter == ","


class TestCatCommandCSVFormatting:
    """Test CSV formatting functionality."""

    @patch("requests.get")
    def test_format_csv_basic(self, mock_get, cat_command):
        """Test basic CSV formatting."""
        csv_content = b"name,age,city\nJohn,30,NYC\nJane,25,LA"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.csv")

        assert "name" in result
        assert "age" in result
        assert "city" in result
        assert "John" in result
        assert "Jane" in result

    @patch("requests.get")
    def test_format_csv_with_line_limit(self, mock_get, cat_command):
        """Test CSV formatting with line limit."""
        csv_content = b"col1,col2\n1,a\n2,b\n3,c\n4,d\n5,e"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.csv", num_lines=2)

        # Should show only first 2 rows
        assert "1" in result
        assert "2" in result
        # Should not show rows 3, 4, 5
        lines = result.split("\n")
        assert not any("3" in line and "c" in line for line in lines)

    @patch("requests.get")
    def test_format_csv_raw_mode(self, mock_get, cat_command):
        """Test CSV in raw mode (no formatting)."""
        csv_content = b"name,age\nJohn,30"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.csv", raw=True)

        # Should be raw CSV text, not formatted table
        assert result == "name,age\nJohn,30"

    @patch("requests.get")
    def test_format_csv_pandas_format(self, mock_get, cat_command):
        """Test CSV with pandas format output."""
        csv_content = b"name,age\nJohn,30"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.csv", format_type="pandas")

        # Should contain DataFrame string representation
        assert "name" in result
        assert "age" in result


class TestCatCommandParquetFormatting:
    """Test Parquet formatting functionality."""

    @patch("requests.get")
    def test_format_parquet_basic(self, mock_get, cat_command):
        """Test basic Parquet formatting."""
        # Create a simple parquet file in memory
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        parquet_content = buffer.getvalue()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = parquet_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.parquet")

        assert "name" in result
        assert "age" in result
        assert "Alice" in result
        assert "Bob" in result

    @patch("requests.get")
    def test_format_parquet_with_line_limit(self, mock_get, cat_command):
        """Test Parquet formatting with line limit."""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "value": ["a", "b", "c", "d", "e"]
        })
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        parquet_content = buffer.getvalue()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = parquet_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.parquet", num_lines=2)

        # Should show only first 2 rows
        assert "1" in result
        assert "2" in result

    @patch("requests.get")
    def test_format_parquet_pandas_format(self, mock_get, cat_command):
        """Test Parquet with pandas format output."""
        df = pd.DataFrame({"name": ["Alice"], "age": [25]})
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        parquet_content = buffer.getvalue()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = parquet_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.parquet", format_type="pandas")

        # Should contain DataFrame string representation
        assert "name" in result
        assert "age" in result


class TestCatCommandRawFormatting:
    """Test raw text formatting."""

    @patch("requests.get")
    def test_format_raw_text(self, mock_get, cat_command):
        """Test raw text file display."""
        text_content = b"This is a plain text file.\nWith multiple lines.\nLine 3."
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = text_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.txt")

        assert result == "This is a plain text file.\nWith multiple lines.\nLine 3."

    @patch("requests.get")
    def test_format_raw_with_line_limit(self, mock_get, cat_command):
        """Test raw text with line limit."""
        text_content = b"Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = text_content
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/test.txt", num_lines=3)

        assert result == "Line 1\nLine 2\nLine 3"


class TestCatCommandErrorHandling:
    """Test error handling in cat command."""

    @patch("requests.get")
    def test_csv_parsing_failure_fallback(self, mock_get, cat_command):
        """Test fallback to raw display when CSV parsing fails."""
        malformed_csv = b"name,age\nJohn,30,extra,columns\nJane"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = malformed_csv
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/bad.csv")

        # Should still return something (either formatted or raw)
        assert result is not None
        assert len(result) > 0

    @patch("requests.get")
    def test_http_error_handling(self, mock_get, cat_command):
        """Test HTTP error handling."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("File not found")
        mock_get.return_value = mock_response

        result = cat_command.execute("/data/nonexistent.csv")

        assert "Error" in result
        assert "File not found" in result


# ============================================================================
# EDGE CASES FOR COMPLETE COVERAGE
# ============================================================================


class TestCatEdgeCasesForCoverage:
    """Edge case tests for complete code coverage."""

    def test_csv_detection_with_newline(self):
        """Test CSV detection when newline exists in content."""
        from unittest.mock import Mock

        from webhdfsmagic.commands.file_ops import CatCommand
        mock_client = Mock()
        cat_cmd = CatCommand(mock_client)

        content = b"name,age\nJohn,30\nJane,25"
        file_type = cat_cmd._detect_file_type("/data/file.csv", content)
        assert file_type == "csv"

    def test_csv_detection_without_newline(self):
        """Test CSV detection without newline (single line)."""
        from unittest.mock import Mock

        from webhdfsmagic.commands.file_ops import CatCommand
        mock_client = Mock()
        cat_cmd = CatCommand(mock_client)

        content = b"name,age,city,country"
        file_type = cat_cmd._detect_file_type("/data/file.csv", content)
        assert file_type == "csv"


class TestCatFormatMethods:
    """Test internal formatting methods."""

    @pytest.fixture
    def cat_command(self):
        """Create a CatCommand instance."""
        from unittest.mock import MagicMock

        from webhdfsmagic.client import WebHDFSClient
        from webhdfsmagic.commands.file_ops import CatCommand

        client = MagicMock(spec=WebHDFSClient)
        client.knox_url = "https://knox.example.com"
        client.webhdfs_api = "/webhdfs/v1"
        client.auth_user = "testuser"
        client.auth_password = "testpass"
        client.verify_ssl = False
        return CatCommand(client)

    def test_format_csv_with_truncation_notice(self, cat_command):
        """Test CSV formatting with truncation notice."""
        csv_data = "col1,col2,col3\n" + "\n".join(
            [f"{i},value{i},data{i}" for i in range(20)]
        )
        content = csv_data.encode('utf-8')

        result = cat_command._format_csv(content, num_lines=10, format_type='table')

        assert "showing first 10" in result
        assert "col1" in result

    def test_format_csv_pandas_format(self, cat_command):
        """Test CSV formatting with pandas format."""
        csv_data = "name,age,city\nAlice,30,NYC\nBob,25,LA"
        content = csv_data.encode('utf-8')

        result = cat_command._format_csv(content, num_lines=-1, format_type='pandas')

        assert "name" in result
        assert "Alice" in result

    def test_format_csv_error_fallback(self, cat_command):
        """Test CSV formatting error fallback."""
        content = b"\xff\xfe\x00\x00"

        result = cat_command._format_csv(content, num_lines=-1, format_type='table')

        assert "[CSV parsing failed:" in result

    def test_format_parquet_with_truncation(self, cat_command):
        """Test Parquet formatting with row limit."""
        import io

        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = pd.DataFrame({'id': range(20), 'value': [f'val_{i}' for i in range(20)]})

        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        content = buffer.getvalue()

        result = cat_command._format_parquet(content, num_lines=5, format_type='table')

        assert "showing first 5" in result
        assert "id" in result

    def test_format_parquet_pandas_format(self, cat_command):
        """Test Parquet formatting with pandas format."""
        import io

        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})

        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        content = buffer.getvalue()

        result = cat_command._format_parquet(
            content, num_lines=-1, format_type='pandas'
        )

        assert "name" in result
        assert "Alice" in result

    def test_format_parquet_no_truncation(self, cat_command):
        """Test Parquet formatting without truncation."""
        import io

        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = pd.DataFrame({'col': [1, 2, 3]})

        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)
        content = buffer.getvalue()

        result = cat_command._format_parquet(content, num_lines=-1, format_type='table')

        assert "col" in result
        assert "showing first" not in result

    def test_infer_delimiter_inconsistent_counts(self, cat_command):
        """Test delimiter inference with inconsistent line counts."""
        content = "a,b,c\nd,e\nf,g,h,i\n"

        delimiter = cat_command._infer_delimiter(content)

        assert delimiter == ','

    def test_infer_delimiter_no_delimiters(self, cat_command):
        """Test delimiter inference with no delimiters."""
        content = "single line\nanother line\n"

        delimiter = cat_command._infer_delimiter(content)

        assert delimiter == ','

    def test_infer_delimiter_all_zero_counts(self, cat_command):
        """Test delimiter inference when all counts are zero."""
        content = "abc\ndef\n"

        delimiter = cat_command._infer_delimiter(content)

        assert delimiter == ','

    def test_detect_json_file_type(self, cat_command):
        """Test detection of JSON file type."""
        content = b'{"key": "value"}'

        file_type = cat_command._detect_file_type('/data/test.json', content)

        assert file_type == 'json'

    def test_detect_csv_from_content_with_comma(self, cat_command):
        """Test CSV detection from content with commas."""
        content = b'col1,col2,col3\nval1,val2,val3\n'

        file_type = cat_command._detect_file_type('/data/test.txt', content)

        assert file_type == 'csv'

    def test_detect_csv_from_content_with_tab(self, cat_command):
        """Test CSV detection from content with tabs."""
        content = b'col1\tcol2\tcol3\nval1\tval2\tval3\n'

        file_type = cat_command._detect_file_type('/data/test.txt', content)

        assert file_type == 'csv'

    def test_detect_csv_from_content_no_newline(self, cat_command):
        """Test CSV detection from content without newline."""
        content = b'col1,col2,col3'

        file_type = cat_command._detect_file_type('/data/test.txt', content)

        assert file_type == 'csv'

    def test_detect_text_for_non_csv_content(self, cat_command):
        """Test text detection for non-CSV content."""
        content = b'Just some text without delimiters'

        file_type = cat_command._detect_file_type('/data/test.txt', content)

        assert file_type == 'text'

    def test_detect_handles_exception_in_content_check(self, cat_command):
        """Test detection handles exceptions gracefully."""
        content = b'\xff\xfe'

        file_type = cat_command._detect_file_type('/data/test.txt', content)

        assert file_type == 'text'

    def test_execute_with_307_redirect(self, cat_command):
        """Test execute with 307 redirect."""
        from unittest.mock import Mock, patch

        with patch('requests.get') as mock_get:
            redirect_response = Mock()
            redirect_response.status_code = 307
            redirect_response.headers = {
                'Location': 'http://datanode:50075/webhdfs/v1/file.txt?op=OPEN'
            }

            final_response = Mock()
            final_response.status_code = 200
            final_response.content = b'test content'
            final_response.raise_for_status = Mock()

            mock_get.side_effect = [redirect_response, final_response]

            result = cat_command.execute('/test.txt')

            assert 'test content' in result

    def test_execute_handles_empty_content(self, cat_command):
        """Test execute with empty content."""
        from unittest.mock import Mock, patch

        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = b''
            mock_response.raise_for_status = Mock()

            mock_get.return_value = mock_response

            result = cat_command.execute('/empty.txt')

            assert result == ''
