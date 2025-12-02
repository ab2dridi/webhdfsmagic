import os
import json
import urllib.parse
import requests
from datetime import datetime
from IPython.core.magic import Magics, magics_class, line_magic
from traitlets import Unicode, Bool
import pandas as pd
from typing import Dict, Any, Union
from IPython.display import HTML  # For HTML display
import glob
import fnmatch
import traceback

@magics_class
class WebHDFSMagics(Magics):
    """
    IPython magics pour interagir avec HDFS via WebHDFS/Knox.

    Ce module fournit les commandes magiques suivantes :
      - %hdfs ls      : Lister les fichiers sur HDFS.
      - %hdfs mkdir   : Créer un répertoire sur HDFS.
      - %hdfs rm      : Supprimer des fichiers/répertoires (wildcards supportés).
      - %hdfs put     : Envoyer des fichiers locaux vers HDFS en streaming (gros fichiers).
      - %hdfs get     : Télécharger des fichiers depuis HDFS en streaming (gros fichiers).
      - %hdfs cat     : Afficher le contenu d'un fichier depuis HDFS (avec option -n).
      - %hdfs chmod   : Modifier les permissions d'un fichier/répertoire.
      - %hdfs chown   : Changer le propriétaire et le groupe.

    La configuration (URL Knox, API WebHDFS, identifiants, verify_ssl, etc.)
    est d'abord chargée depuis ~/.webhdfsmagic/config.json, puis, si absent,
    depuis ~/.sparkmagic/config.json. Pour Sparkmagic, l'URL est transformée
    pour ne conserver que la partie de base (ex : "https://hostname:port/gateway/default")
    et y ajouter "/webhdfs/v1". Le paramètre verify_ssl peut être un booléen
    ou le chemin d'accès à un certificat. Par défaut (pour Sparkmagic), verify_ssl est False.
    """

    # Valeurs par défaut (modifiables via configuration ou la commande setconfig)
    knox_url = Unicode("https://localhost:8443/gateway/default").tag(config=True)
    webhdfs_api = Unicode("/webhdfs/v1").tag(config=True)
    auth_user = Unicode().tag(config=True)
    auth_password = Unicode().tag(config=True)
    verify_ssl = Bool(False).tag(config=True)

    def __init__(self, shell):
        """
        Initialise l'extension et charge la configuration externe.
        
        :param shell: Le shell IPython.
        """
        super().__init__(shell)
        self._load_external_config()

    def _load_external_config(self) -> None:
        """
        Charge la configuration depuis ~/.webhdfsmagic/config.json s'il existe;
        sinon, depuis ~/.sparkmagic/config.json.
        
        Pour Sparkmagic, on extrait l'URL jusqu'au dernier segment et on y
        ajoute "/webhdfs/v1". Le paramètre verify_ssl peut être un booléen ou
        un chemin vers un certificat. Par défaut, pour Sparkmagic, verify_ssl vaut False.
        """
        config_path_webhdfsmagic = os.path.expanduser("~/.webhdfsmagic/config.json")
        config_path_sparkmagic = os.path.expanduser("~/.sparkmagic/config.json")
        config = None
        default_verify_ssl = False

        if os.path.exists(config_path_webhdfsmagic):
            try:
                with open(config_path_webhdfsmagic) as f:
                    config = json.load(f)
                print("Loading configuration from", config_path_webhdfsmagic)
            except Exception as e:
                print(f"Warning loading configuration file {config_path_webhdfsmagic}: {str(e)}")
        elif os.path.exists(config_path_sparkmagic):
            try:
                with open(config_path_sparkmagic) as f:
                    config = json.load(f)
                print("Loading configuration from", config_path_sparkmagic)
                creds = config.get("kernel_python_credentials", {})
                sparkmagic_url = creds.get("url", "")
                if sparkmagic_url:
                    parsed = urllib.parse.urlsplit(sparkmagic_url)
                    # Retirer le dernier segment du chemin
                    path_parts = parsed.path.rstrip("/").split("/")
                    if len(path_parts) > 1 and path_parts[-1]:
                        base_path = "/".join(path_parts[:-1])
                    else:
                        base_path = parsed.path
                    base_url = f"{parsed.scheme}://{parsed.netloc}{base_path}"
                    # Définir l'URL Knox en ajoutant "/webhdfs/v1"
                    self.knox_url = base_url + "/webhdfs/v1"
                self.auth_user = creds.get("username", self.auth_user)
                self.auth_password = creds.get("password", self.auth_password)
                self.verify_ssl = config.get("verify_ssl", default_verify_ssl)
            except Exception as e:
                print(f"Warning loading configuration file {config_path_sparkmagic}: {str(e)}")
        else:
            print("No configuration file found. Using default settings.")

        # Si la configuration provient du fichier .webhdfsmagic, on met à jour.
        if config and os.path.exists(config_path_webhdfsmagic):
            self.knox_url = config.get("knox_url", self.knox_url)
            self.webhdfs_api = config.get("webhdfs_api", self.webhdfs_api)
            self.auth_user = config.get("username", self.auth_user)
            self.auth_password = config.get("password", self.auth_password)
            self.verify_ssl = config.get("verify_ssl", default_verify_ssl)

        # Gestion de verify_ssl :
        if isinstance(self.verify_ssl, bool):
            pass
        elif isinstance(self.verify_ssl, str):
            if not os.path.exists(self.verify_ssl):
                print(f"Warning: certificate file '{self.verify_ssl}' does not exist. Falling back to False.")
                self.verify_ssl = False
        else:
            print("Warning: verify_ssl has an unexpected type. Using default value False.")
            self.verify_ssl = False

    def _execute(self, method: str, operation: str, path: str, **params) -> Dict[str, Any]:
        """
        Exécute une requête WebHDFS.
        
        :param method: Méthode HTTP (GET, PUT, DELETE, etc.).
        :param operation: Opération WebHDFS (e.g. LISTSTATUS, MKDIRS, DELETE).
        :param path: Chemin sur HDFS.
        :param params: Paramètres supplémentaires pour la requête.
        :return: Réponse JSON décodée.
        :raises HTTPError: En cas d'erreur HTTP.
        """
        url = f"{self.knox_url}{self.webhdfs_api}{path}"
        params["op"] = operation

        response = requests.request(
            method=method,
            url=url,
            params=params,
            auth=(self.auth_user, self.auth_password),
            verify=self.verify_ssl
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def _format_perms(self, perm: int) -> str:
        """
        Convertit une permission numérique en chaîne de type UNIX (e.g., "rwx").
        
        :param perm: Permission sous forme d'entier.
        :return: Chaîne de permissions.
        """
        return "".join(
            ['r' if perm & 4 else '-', 
             'w' if perm & 2 else '-', 
             'x' if perm & 1 else '-']
        )

    def _format_ls(self, path: str) -> pd.DataFrame:
        """
        Formate la sortie de la commande LISTSTATUS en DataFrame Pandas.
        
        :param path: Chemin HDFS à lister.
        :return: DataFrame des informations de fichiers.
        """
        result = self._execute("GET", "LISTSTATUS", path)
        entries = []
        for f in result["FileStatuses"]["FileStatus"]:
            permission_int = int(f["permission"], 8)
            entry = {
                "name": f["pathSuffix"],
                "type": "DIR" if f["type"] == "DIRECTORY" else "FILE",
                "size": f.get("length", 0),
                "owner": f["owner"],
                "group": f["group"],
                "permissions": f"{self._format_perms((permission_int >> 6) & 7)}"
                               f"{self._format_perms((permission_int >> 3) & 7)}"
                               f"{self._format_perms(permission_int & 7)}",
                "block_size": f.get("blockSize", 0),
                "modified": datetime.fromtimestamp(f["modificationTime"] / 1000),
                "replication": f.get("replication", 1)
            }
            entries.append(entry)
        return pd.DataFrame(entries)

    def _set_permission(self, path: str, permission: str) -> str:
        """
        Définit les permissions d'un fichier ou répertoire via SETPERMISSION.
        
        :param path: Chemin HDFS.
        :param permission: Permissions à appliquer.
        :return: Message de confirmation.
        """
        self._execute("PUT", "SETPERMISSION", path, permission=permission)
        return f"Permission {permission} set for {path}"

    def _recursive_set_permission(self, path: str, permission: str) -> None:
        """
        Applique récursivement une modification des permissions à un répertoire et son contenu.
        
        :param path: Chemin HDFS.
        :param permission: Permissions à appliquer.
        """
        self._set_permission(path, permission)
        try:
            df = self._format_ls(path)
        except Exception:
            return
        for _, row in df.iterrows():
            full_path = path.rstrip('/') + '/' + row['name']
            if row['type'] == 'DIR':
                self._recursive_set_permission(full_path, permission)
            else:
                self._set_permission(full_path, permission)

    def _set_owner(self, path: str, owner: str, group: str) -> str:
        """
        Définit le propriétaire et le groupe d'un fichier/répertoire via SETOWNER.
        
        :param path: Chemin HDFS.
        :param owner: Nouveau propriétaire.
        :param group: Nouveau groupe.
        :return: Message de confirmation.
        """
        self._execute("PUT", "SETOWNER", path, owner=owner, group=group)
        return f"Owner {owner}:{group} set for {path}"

    def _recursive_set_owner(self, path: str, owner: str, group: str) -> None:
        """
        Applique récursivement un changement de propriétaire et de groupe à un répertoire et son contenu.
        
        :param path: Chemin HDFS.
        :param owner: Nouveau propriétaire.
        :param group: Nouveau groupe.
        """
        self._set_owner(path, owner, group)
        try:
            df = self._format_ls(path)
        except Exception:
            return
        for _, row in df.iterrows():
            full_path = path.rstrip('/') + '/' + row['name']
            if row['type'] == 'DIR':
                self._recursive_set_owner(full_path, owner, group)
            else:
                self._set_owner(full_path, owner, group)

    @line_magic
    def hdfs(self, line: str) -> Union[pd.DataFrame, str, HTML]:
        """
        Point d'entrée principal pour les commandes magiques %hdfs.

        Commandes supportées : help, setconfig, ls, mkdir, rm, put, get, cat, chmod, chown.
        
        :param line: Ligne de commande saisie par l'utilisateur.
        :return: Résultat de la commande ou aide HTML.
        """
        parts = line.strip().split()
        if not parts:
            return self._help()

        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "help":
            return self._help()

        if cmd == "setconfig":
            try:
                config_json = " ".join(args)
                config = json.loads(config_json)
                self.knox_url = config.get("knox_url", self.knox_url)
                self.webhdfs_api = config.get("webhdfs_api", self.webhdfs_api)
                self.auth_user = config.get("username", self.auth_user)
                self.auth_password = config.get("password", self.auth_password)
                self.verify_ssl = config.get("verify_ssl", self.verify_ssl)
                return "Configuration updated successfully."
            except Exception as e:
                return f"Configuration error: {str(e)}"

        try:
            if cmd == "ls":
                path = args[0] if args else "/"
                return self._format_ls(path)

            elif cmd == "mkdir":
                return self._execute("PUT", "MKDIRS", args[0])

            elif cmd == "rm":
                rm_path_pattern = args[0]
                recursive = "-r" in args
                if "*" in rm_path_pattern or "?" in rm_path_pattern:
                    base_dir = os.path.dirname(rm_path_pattern)
                    pattern = os.path.basename(rm_path_pattern)
                    df = self._format_ls(base_dir)
                    matching_files = df[df['name'].apply(lambda x: fnmatch.fnmatch(x, pattern))]
                    if matching_files.empty:
                        return f"No file matches the pattern {rm_path_pattern}"
                    responses = []
                    for _, row in matching_files.iterrows():
                        full_path = base_dir.rstrip('/') + '/' + row['name']
                        try:
                            self._execute("DELETE", "DELETE", full_path, recursive="true" if recursive else "false")
                            responses.append(f"{full_path} deleted")
                        except Exception as e:
                            responses.append(f"Error for {full_path}: {str(e)}")
                    return "\n".join(responses)
                else:
                    return self._execute("DELETE", "DELETE", rm_path_pattern, recursive="true" if recursive else "false")

            elif cmd == "put":
                local_file_pattern = args[0]
                hdfs_path = args[1]
                local_files = glob.glob(os.path.expanduser(local_file_pattern))
                if not local_files:
                    return f"No file matches the pattern {local_file_pattern}"
                responses = []
                for local_file in local_files:
                    if not os.path.isabs(local_file):
                        local_file = os.path.join(os.getcwd(), local_file)
                    final_hdfs_path = hdfs_path
                    if hdfs_path.endswith('/') or hdfs_path.endswith('.'):
                        if hdfs_path.endswith('/.'):
                            final_hdfs_path = hdfs_path[:-1]
                        if not final_hdfs_path.endswith('/'):
                            final_hdfs_path += '/'
                        final_hdfs_path = final_hdfs_path + os.path.basename(local_file)
                    with open(local_file, "rb") as f:
                        # Step 1: Initialize file creation to get the redirect URL.
                        init_url = f"{self.knox_url}{self.webhdfs_api}{final_hdfs_path}"
                        init_params = {"op": "CREATE", "overwrite": "true"}
                        init_response = requests.put(
                            init_url,
                            params=init_params,
                            auth=(self.auth_user, self.auth_password),
                            verify=self.verify_ssl,
                            allow_redirects=False
                        )
                        if init_response.status_code == 307:
                            redirect_url = init_response.headers.get("Location")
                            if not redirect_url:
                                responses.append(f"Error for {local_file}: Missing redirect URL.")
                                continue
                            # Step 2: Upload the file using streaming by passing the file handle.
                            upload_response = requests.put(
                                redirect_url,
                                data=f,
                                auth=(self.auth_user, self.auth_password),
                                verify=self.verify_ssl
                            )
                            try:
                                upload_response.raise_for_status()
                                responses.append(f"{local_file} uploaded successfully to {final_hdfs_path}")
                            except Exception as e:
                                responses.append(f"Error for {local_file}: {str(e)}")
                        else:
                            responses.append(f"Initiation failed for {local_file}, status: {init_response.status_code}")
                return "\n".join(responses)

            elif cmd == "get":
                hdfs_source_pattern = args[0]
                local_dest = args[1]
                if "*" in hdfs_source_pattern or "?" in hdfs_source_pattern:
                    base_dir = os.path.dirname(hdfs_source_pattern)
                    pattern = os.path.basename(hdfs_source_pattern)
                    df = self._format_ls(base_dir)
                    matching_files = df[df['name'].apply(lambda x: fnmatch.fnmatch(x, pattern))]
                    if matching_files.empty:
                        return f"No file matches the pattern {hdfs_source_pattern}"
                    responses = []
                    for _, row in matching_files.iterrows():
                        file_name = row['name']
                        hdfs_file = base_dir.rstrip('/') + '/' + file_name
                        final_local_dest = local_dest
                        if local_dest in [".", "~", "~/"]:
                            final_local_dest = os.path.join(os.getcwd() if local_dest=="." else os.path.expanduser("~"), file_name)
                        elif local_dest.endswith('/') or local_dest.endswith('.'):
                            if not local_dest.endswith('/'):
                                local_dest += '/'
                            final_local_dest = os.path.join(local_dest, file_name)
                        response = requests.get(
                            f"{self.knox_url}{self.webhdfs_api}{hdfs_file}?op=OPEN",
                            auth=(self.auth_user, self.auth_password),
                            verify=self.verify_ssl,
                            stream=True
                        )
                        try:
                            response.raise_for_status()
                            with open(final_local_dest, "wb") as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                            responses.append(f"{hdfs_file} downloaded to {final_local_dest}")
                        except Exception as e:
                            tb = traceback.format_exc()
                            responses.append(f"Error for {hdfs_file}: {str(e)}\nTraceback:\n{tb}")
                    return "\n".join(responses)
                else:
                    if local_dest == ".":
                        local_dest = os.path.join(os.getcwd(), os.path.basename(hdfs_source_pattern))
                    elif local_dest in ["~", "~/"]:
                        local_dest = os.path.join(os.path.expanduser("~"), os.path.basename(hdfs_source_pattern))
                    else:
                        if local_dest.endswith('/') or local_dest.endswith('.'):
                            if not local_dest.endswith('/'):
                                local_dest += '/'
                            local_dest = local_dest + os.path.basename(hdfs_source_pattern)
                    response = requests.get(
                        f"{self.knox_url}{self.webhdfs_api}{hdfs_source_pattern}?op=OPEN",
                        auth=(self.auth_user, self.auth_password),
                        verify=self.verify_ssl,
                        stream=True
                    )
                    try:
                        response.raise_for_status()
                        with open(local_dest, "wb") as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        return f"File downloaded to {local_dest}"
                    except Exception as e:
                        tb = traceback.format_exc()
                        return f"Error: {str(e)}\nTraceback:\n{tb}"

            elif cmd == "cat":
                if not args:
                    return "Usage: %hdfs cat <file> [-n <number_of_lines>]"
                file_path = args[0]
                num_lines = 100
                if len(args) > 1 and args[1] == "-n":
                    try:
                        num_lines = int(args[2])
                    except (IndexError, ValueError):
                        return "Error: invalid number of lines specified."
                url = f"{self.knox_url}{self.webhdfs_api}{file_path}?op=OPEN"
                response = requests.get(
                    url,
                    auth=(self.auth_user, self.auth_password),
                    verify=self.verify_ssl
                )
                try:
                    response.raise_for_status()
                    content = response.content.decode("utf-8", errors="replace")
                    lines = content.splitlines()
                    if num_lines == -1:
                        result = "\n".join(lines)
                    else:
                        result = "\n".join(lines[:num_lines])
                    return result
                except Exception as e:
                    tb = traceback.format_exc()
                    return f"Error: {str(e)}\nTraceback:\n{tb}"

            elif cmd == "chmod":
                recursive = False
                arg_index = 0
                if args[0] == "-R":
                    recursive = True
                    arg_index = 1
                permission = args[arg_index]
                path = args[arg_index + 1]
                if recursive:
                    self._recursive_set_permission(path, permission)
                    return f"Recursive chmod {permission} applied on {path}"
                else:
                    return self._set_permission(path, permission)

            elif cmd == "chown":
                recursive = False
                arg_index = 0
                if args[0] == "-R":
                    recursive = True
                    arg_index = 1
                owner_group = args[arg_index]
                if ":" in owner_group:
                    owner, group = owner_group.split(":", 1)
                else:
                    owner = owner_group
                    group = ""
                path = args[arg_index + 1]
                if recursive:
                    self._recursive_set_owner(path, owner, group)
                    return f"Recursive chown {owner}:{group} applied on {path}"
                else:
                    return self._set_owner(path, owner, group)

            else:
                return f"Unknown command: {cmd}"

        except requests.HTTPError as e:
            return f"HTTP Error {e.response.status_code}: {e.response.text}"
        except Exception as e:
            tb = traceback.format_exc()
            return f"Error: {str(e)}\nTraceback:\n{tb}"

    def _help(self) -> HTML:
        html = """
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr>
                    <th>Command</th>
                    <th>Description</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>%hdfs help</td>
                    <td>Display this help</td>
                </tr>
                <tr>
                    <td>%hdfs setconfig {"knox_url": "...", "webhdfs_api": "...", "username": "...", "password": "...", "verify_ssl": false}</td>
                    <td>Set configuration and credentials directly in the notebook</td>
                </tr>
                <tr>
                    <td>%hdfs ls [path]</td>
                    <td>List files on HDFS</td>
                </tr>
                <tr>
                    <td>%hdfs mkdir &lt;path&gt;</td>
                    <td>Create a directory on HDFS</td>
                </tr>
                <tr>
                    <td>%hdfs rm &lt;path or pattern&gt; [-r]</td>
                    <td>
                        Delete a file/directory. Supports wildcards.<br>
                        Example: %hdfs rm /user/files* [-r]
                    </td>
                </tr>
                <tr>
                    <td>%hdfs put &lt;local_file_or_pattern&gt; &lt;hdfs_destination&gt;</td>
                    <td>
                        Upload one or more local files (wildcards allowed) to HDFS.<br>
                        If the HDFS path ends with '/' or '.', the original file name is preserved.
                    </td>
                </tr>
                <tr>
                    <td>%hdfs get &lt;hdfs_file_or_pattern&gt; &lt;local_destination&gt;</td>
                    <td>
                        Download one or more files from HDFS.<br>
                        If the local destination is a directory (or "."/~), the original file name is appended.
                    </td>
                </tr>
                <tr>
                    <td>%hdfs cat &lt;file&gt; [-n &lt;number_of_lines&gt;]</td>
                    <td>
                        Display file content. Default is 100 lines.<br>
                        Use "-n -1" to display the full file.
                    </td>
                </tr>
                <tr>
                    <td>%hdfs chmod [-R] &lt;permission&gt; &lt;path&gt;</td>
                    <td>
                        Set permissions (SETPERMISSION).<br>
                        The "-R" option applies recursively.
                    </td>
                </tr>
                <tr>
                    <td>%hdfs chown [-R] &lt;user:group&gt; &lt;path&gt;</td>
                    <td>
                        Set owner and group (SETOWNER).<br>
                        The "-R" option applies recursively.
                    </td>
                </tr>
            </tbody>
        </table>
        """
        return HTML(html)

def load_ipython_extension(ipython):
    """
    Fonction d'extension IPython qui enregistre les magics.
    
    :param ipython: L'instance IPython.
    """
    ipython.register_magics(WebHDFSMagics)
