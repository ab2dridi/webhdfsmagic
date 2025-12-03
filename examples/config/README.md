# Configuration Examples for webhdfsmagic

This directory contains example configuration files for different scenarios.

## Quick Start

Copy one of these examples to `~/.webhdfsmagic/config.json` and customize it:

```bash
# Example: Copy the no-SSL configuration
cp examples/config/config_no_ssl.json ~/.webhdfsmagic/config.json

# Edit with your credentials
nano ~/.webhdfsmagic/config.json
```

## Configuration Files

### 1. `config_no_ssl.json` - Disable SSL Verification

**Use case:** Development environment, testing, or when using HTTP instead of HTTPS.

```json
{
  "knox_url": "https://your-knox-gateway:8443/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",  # pragma: allowlist secret
  "verify_ssl": false
}
```

**Note:** Setting `verify_ssl` to `false` disables SSL certificate validation. Use only in trusted networks.

### 2. `config_with_ssl.json` - Enable SSL Verification (Default CA Bundle)

**Use case:** Production environment with valid SSL certificates signed by a known CA.

```json
{
  "knox_url": "https://your-knox-gateway:8443/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",  # pragma: allowlist secret
  "verify_ssl": true
}
```

**Note:** Uses the system's default CA bundle to verify SSL certificates.

### 3. `config_with_cert.json` - Custom Certificate Path

**Use case:** Self-signed certificates or custom CA certificates.

```json
{
  "knox_url": "https://your-knox-gateway:8443/gateway/default",
  "webhdfs_api": "/webhdfs/v1",
  "username": "your_username",
  "password": "your_password",  # pragma: allowlist secret
  "verify_ssl": "/path/to/your/cacert.pem"
}
```

**Certificate path options:**
- Absolute path: `/etc/ssl/certs/my-ca-bundle.crt`
- User home expansion: `~/certs/knox-ca.pem`
- Relative path (not recommended): `./certs/ca.pem`

**How to get your certificate:**

```bash
# Download certificate from server
openssl s_client -showcerts -connect your-knox-gateway:8443 </dev/null 2>/dev/null | \
  openssl x509 -outform PEM > ~/knox-cert.pem

# Or export from your browser (Chrome/Firefox)
# 1. Visit https://your-knox-gateway:8443
# 2. Click the padlock icon → Certificate → Details → Export
# 3. Save as PEM format
```

### 4. `config_minimal.json` - Local Development

**Use case:** Testing with local HDFS cluster (Docker, Hadoop standalone).

```json
{
  "knox_url": "http://localhost:9870",
  "webhdfs_api": "/webhdfs/v1",
  "username": "hdfs",
  "password": "",
  "verify_ssl": false
}
```

**Note:** For local HTTP connections (no SSL), use `http://` in the URL.

## Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `knox_url` | string | Yes | Base URL of Knox gateway (e.g., `https://host:8443/gateway/default`) |
| `webhdfs_api` | string | Yes | WebHDFS API path (usually `/webhdfs/v1`) |
| `username` | string | Yes | Authentication username |
| `password` | string | Yes | Authentication password (can be empty for Kerberos) |
| `verify_ssl` | boolean/string | No | SSL verification: `false`, `true`, or path to certificate file |

## SSL Verification Details

### Option 1: `verify_ssl: false`
- **Security:** ⚠️ Low - Susceptible to man-in-the-middle attacks
- **Use case:** Development, testing, trusted networks only
- **Behavior:** Disables all SSL certificate validation

### Option 2: `verify_ssl: true`
- **Security:** ✅ High - Uses system CA bundle
- **Use case:** Production with valid CA-signed certificates
- **Behavior:** Validates certificate against system's trusted CAs

### Option 3: `verify_ssl: "/path/to/cert.pem"`
- **Security:** ✅ High - Custom certificate validation
- **Use case:** Self-signed or internal CA certificates
- **Behavior:** Validates certificate against specified file
- **File formats:** PEM, CRT, CER (must be PEM-encoded)

**Error handling:**
If the certificate file doesn't exist, webhdfsmagic will:
1. Print a warning message
2. Fall back to `verify_ssl: false`
3. Continue execution (to avoid breaking existing notebooks)

## Testing Your Configuration

After creating your config file, test it:

### In a Jupyter Notebook:

```python
%load_ext webhdfsmagic
%hdfs ls /
```

### In a Python script:

```python
from webhdfsmagic.magics import WebHDFSMagics
from IPython.core.interactiveshell import InteractiveShell

shell = InteractiveShell.instance()
magics = WebHDFSMagics(shell)

# Configuration is loaded automatically from ~/.webhdfsmagic/config.json
print(f"Knox URL: {magics.knox_url}")
print(f"SSL Verification: {magics.verify_ssl}")
```

## Environment-Specific Configurations

### Development
```bash
cp examples/config/config_no_ssl.json ~/.webhdfsmagic/config.json
# Edit with dev credentials
```

### Staging
```bash
cp examples/config/config_with_cert.json ~/.webhdfsmagic/config.json
# Use staging certificate
```

### Production
```bash
cp examples/config/config_with_ssl.json ~/.webhdfsmagic/config.json
# Use production credentials with full SSL verification
```

## Troubleshooting

### SSL Certificate Verification Failed

```
SSLError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed
```

**Solutions:**
1. Set `verify_ssl: false` (temporary, for testing only)
2. Download the server certificate and use `verify_ssl: "/path/to/cert.pem"`
3. Install the CA certificate in your system's trust store

### Certificate File Not Found

```
Warning: certificate file '/path/to/cert.pem' does not exist. Falling back to False.
```

**Solutions:**
1. Check the file path (use absolute path)
2. Verify file permissions (`chmod 644 cert.pem`)
3. Expand `~` to full path: `/Users/yourname/certs/cert.pem`

### Authentication Failed

```
HTTPError: 401 Unauthorized
```

**Solutions:**
1. Verify username and password are correct
2. Check if account is locked or expired
3. Verify Knox authentication method (Basic Auth vs Kerberos)

## Security Best Practices

1. **Never commit credentials to git:**
   ```bash
   echo "~/.webhdfsmagic/config.json" >> ~/.gitignore
   ```

2. **Use environment variables for sensitive data:**
   ```python
   import os
   import json

   config = {
       "knox_url": os.getenv("KNOX_URL"),
       "username": os.getenv("KNOX_USER"),
       "password": os.getenv("KNOX_PASS"),
       "verify_ssl": True
   }
   ```

3. **Restrict file permissions:**
   ```bash
   chmod 600 ~/.webhdfsmagic/config.json
   ```

4. **Use SSL in production:**
   Always set `verify_ssl: true` or provide a certificate path in production.

5. **Rotate passwords regularly:**
   Update your config.json when credentials change.

## Alternative: Sparkmagic Configuration

If you already have `~/.sparkmagic/config.json`, webhdfsmagic can use it as a fallback:

```json
{
  "kernel_python_credentials": {
    "username": "your_username",
    "password": "your_password",  # pragma: allowlist secret
    "url": "https://hostname:port/gateway/default/livy_for_spark3",
    "auth": "Basic_Access"
  }
}
```

webhdfsmagic will:
1. Extract the base URL (removes `/livy_for_spark3`)
2. Append `/webhdfs/v1`
3. Set `verify_ssl: false` by default

To override SSL settings with Sparkmagic, create `~/.webhdfsmagic/config.json` (takes priority).

## Further Reading

- [WebHDFS REST API Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
- [Apache Knox Documentation](https://knox.apache.org/)
- [Python Requests SSL Verification](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification)
