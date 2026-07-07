from __future__ import annotations

import base64
import sys
import types

import pytest

from lakelogic.engines import catalog_resolver as cr
from lakelogic.engines import cloud_credentials as cc


def test_catalog_resolver_core_paths_and_cache(monkeypatch):
    resolver = cr.UnityCatalogResolver(host="https://workspace", token="token", use_databricks_sdk=False)
    assert resolver.is_unity_catalog_table("main.default.orders") is True
    assert resolver.is_unity_catalog_table("s3://bucket/orders") is False
    assert resolver.is_unity_catalog_table("main.default") is False

    monkeypatch.setattr(resolver, "_resolve_with_api", lambda table: f"s3://bucket/{table}/")
    assert resolver.resolve_table("main.default.orders") == "s3://bucket/main.default.orders/"
    assert resolver.resolve_table("main.default.orders") == "s3://bucket/main.default.orders/"

    with pytest.raises(ValueError, match="Invalid Unity Catalog table name"):
        resolver.resolve_table("orders")

    missing = cr.UnityCatalogResolver(host=None, token=None)
    with pytest.raises(ValueError, match="credentials not configured"):
        missing.resolve_table("main.default.orders")

    resolver.clear_cache()
    assert resolver._cache == {}


def test_catalog_resolver_sdk_api_and_convenience_functions(monkeypatch):
    fake_sdk = types.ModuleType("databricks.sdk")

    class FakeWorkspaceClient:
        def __init__(self, host, token):
            self.host = host
            self.token = token
            self.tables = types.SimpleNamespace(
                get=lambda full_name: types.SimpleNamespace(storage_location=f"abfss://resolved/{full_name}")
            )

    fake_sdk.WorkspaceClient = FakeWorkspaceClient
    monkeypatch.setitem(sys.modules, "databricks.sdk", fake_sdk)

    sdk_resolver = cr.UnityCatalogResolver(host="https://workspace", token="token", use_databricks_sdk=True)
    assert sdk_resolver._resolve_with_sdk("main.default.orders") == "abfss://resolved/main.default.orders"

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    fake_requests = types.ModuleType("requests")
    fake_requests.exceptions = types.SimpleNamespace(RequestException=RuntimeError)
    fake_requests.get = lambda url, headers: FakeResponse({"storage_location": "abfss://api/orders"})
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    api_resolver = cr.UnityCatalogResolver(host="https://workspace", token="token", use_databricks_sdk=False)
    assert api_resolver._resolve_with_api("main.default.orders") == "abfss://api/orders"

    monkeypatch.setattr(
        cr,
        "get_unity_catalog_resolver",
        lambda: types.SimpleNamespace(
            is_unity_catalog_table=lambda path: path == "main.default.orders",
            resolve_table=lambda path: "abfss://resolved/orders",
        ),
    )
    assert cr.resolve_unity_catalog_path("main.default.orders") == "abfss://resolved/orders"
    assert cr.resolve_unity_catalog_path("local/path") == "local/path"

    assert cr._resolve_fabric_table("workspace.lakehouse.orders").startswith(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/orders/"
    )
    monkeypatch.setenv("SYNAPSE_STORAGE_ACCOUNT", "synstorage")
    assert cr._resolve_synapse_table("sales.dbo.orders") == "abfss://sales@synstorage.dfs.core.windows.net/dbo/orders/"
    monkeypatch.delenv("SYNAPSE_STORAGE_ACCOUNT")
    assert cr._resolve_synapse_table("sales.dbo.orders") == "sales.dbo.orders"

    monkeypatch.setattr(cr, "resolve_unity_catalog_path", lambda path: f"unity::{path}")
    assert cr.resolve_catalog_path("main.default.orders", platform="unity") == "unity::main.default.orders"
    assert cr.resolve_catalog_path("workspace.lakehouse.orders", platform="fabric").startswith("abfss://workspace@")
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace")
    assert cr.resolve_catalog_path("main.default.orders") == "unity::main.default.orders"
    monkeypatch.delenv("DATABRICKS_HOST")
    assert cr.resolve_catalog_path("sales.dbo.orders", platform="synapse") == "sales.dbo.orders"
    assert cr.resolve_catalog_path("c:/data/orders") == "c:/data/orders"


def test_cloud_credential_resolution_paths(monkeypatch):
    resolver = cc.CloudCredentialResolver(use_key_vault=True)

    assert resolver.resolve_storage_options("abfss://x", {"BEARER_TOKEN": "abc"}) == {"bearer_token": "abc"}
    assert resolver.resolve_storage_options("abfss://x", {"AZURE_STORAGE_SAS_TOKEN": "sas"}) == {"sas_token": "sas"}

    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "acct")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "key")
    azure_opts = resolver.resolve_storage_options("abfss://x")
    assert azure_opts["account_name"] == "acct"
    assert azure_opts["account_key"] == "key"

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    assert resolver.resolve_storage_options("s3://bucket") == {
        "AWS_ACCESS_KEY_ID": "aws-key",
        "AWS_SECRET_ACCESS_KEY": "aws-secret",
        "AWS_REGION": "eu-west-1",
    }

    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/key.json")
    assert resolver.resolve_storage_options("gs://bucket") == {"GOOGLE_SERVICE_ACCOUNT": "/tmp/key.json"}
    assert resolver.resolve_storage_options("file:///tmp/data") == {}


def test_cloud_credential_sp_token_secret_backends_and_cache(monkeypatch):
    resolver = cc.CloudCredentialResolver(
        azure_client_id="client",
        azure_client_secret="secret",
        azure_tenant_id="tenant",
    )

    fake_identity = types.ModuleType("azure.identity")

    class FakeClientSecretCredential:
        def __init__(self, tenant_id, client_id, client_secret):
            self.tenant_id = tenant_id
            self.client_id = client_id
            self.client_secret = client_secret

        def get_token(self, scope):
            return types.SimpleNamespace(token=f"token::{scope}")

    class FakeDefaultAzureCredential:
        def get_token(self, scope):
            return types.SimpleNamespace(token=f"default::{scope}")

    fake_identity.ClientSecretCredential = FakeClientSecretCredential
    fake_identity.DefaultAzureCredential = FakeDefaultAzureCredential
    monkeypatch.setitem(sys.modules, "azure.identity", fake_identity)

    token = resolver._get_sp_token()
    assert token == "token::https://storage.azure.com/.default"
    assert resolver._get_sp_token() == token

    resolver.clear_cache()
    azure_opts = resolver._resolve_azure_credentials({})
    assert azure_opts["bearer_token"] == "token::https://storage.azure.com/.default"

    fake_kv = types.ModuleType("azure.keyvault.secrets")

    class FakeSecretClient:
        def __init__(self, vault_url, credential):
            self.vault_url = vault_url
            self.credential = credential

        def get_secret(self, name):
            return types.SimpleNamespace(value=f"kv::{name}")

    fake_kv.SecretClient = FakeSecretClient
    monkeypatch.setitem(sys.modules, "azure.keyvault.secrets", fake_kv)
    monkeypatch.setenv("AZURE_KEY_VAULT_URL", "https://vault.vault.azure.net/")
    assert resolver._get_azure_key_vault_secret("db-password") == "kv::db-password"

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.Session = lambda: types.SimpleNamespace(
        get_credentials=lambda: types.SimpleNamespace(access_key="a", secret_key="b", token="c"),
        region_name="us-east-1",
    )
    fake_boto3.client = lambda service, region_name=None: types.SimpleNamespace(
        get_secret_value=lambda SecretId: {"SecretString": '{"password": "aws-secret"}'}
    )
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    aws_opts = resolver._resolve_aws_credentials({})
    assert aws_opts["AWS_ACCESS_KEY_ID"] == "a"
    assert resolver._get_aws_secret("secret-name") == "aws-secret"

    fake_secretmanager = types.ModuleType("google.cloud.secretmanager")
    fake_secretmanager.SecretManagerServiceClient = lambda: types.SimpleNamespace(
        access_secret_version=lambda request: types.SimpleNamespace(payload=types.SimpleNamespace(data=b"gcp-secret"))
    )
    fake_google_cloud = types.ModuleType("google.cloud")
    fake_google_cloud.secretmanager = fake_secretmanager
    fake_google = types.ModuleType("google")
    fake_google.cloud = fake_google_cloud
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_google_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", fake_secretmanager)

    fake_google_auth = types.ModuleType("google.auth")
    fake_google_auth.default = lambda: (object(), "project-1")
    monkeypatch.setitem(sys.modules, "google.auth", fake_google_auth)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert resolver._resolve_gcp_credentials({}) == {}

    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "project-1")
    assert resolver._get_gcp_secret("api-key") == "gcp-secret"

    monkeypatch.setattr(resolver, "_get_azure_key_vault_secret", lambda name, vault: f"secret::{name}")
    assert resolver.get_secret("token", "https://vault.vault.azure.net/") == "secret::token"
    assert resolver.get_secret("token", "https://vault.vault.azure.net/") == "secret::token"


def test_cloud_global_helpers_and_databricks_resolver(monkeypatch, tmp_path):
    cc._global_resolver = None

    resolver_a = cc.get_credential_resolver()
    resolver_b = cc.get_credential_resolver()
    assert resolver_a is resolver_b

    calls = []

    class FakeResolver:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def resolve_storage_options(self, path, storage_options=None):
            calls.append((self.kwargs, path, storage_options))
            return {"path": path, **(storage_options or {})}

    monkeypatch.setattr(cc, "CloudCredentialResolver", FakeResolver)
    assert cc.resolve_storage_options("abfss://x", {"a": 1}, azure_client_id="cid") == {"path": "abfss://x", "a": 1}
    assert calls[0][0]["azure_client_id"] == "cid"

    fake_dbutils = types.SimpleNamespace(
        secrets=types.SimpleNamespace(
            get=lambda scope, key: {
                ("lakelogic", "AZURE_CLIENT_ID"): "client",
                ("lakelogic", "AZURE_CLIENT_SECRET"): "secret",
                ("lakelogic", "AZURE_TENANT_ID"): "tenant",
                ("lakelogic-aws", "AWS_ACCESS_KEY_ID"): "aws-key",
                ("lakelogic-aws", "AWS_SECRET_ACCESS_KEY"): "aws-secret",
                ("lakelogic-aws", "AWS_REGION"): "us-west-2",
                ("lakelogic-gcp", "GCP_SERVICE_ACCOUNT_KEY"): '{"type":"service_account"}',
                ("lakelogic-gcp", "GOOGLE_CLOUD_PROJECT"): "project-1",
            }[(scope, key)]
        )
    )

    azure_resolver = cc.DatabricksSecretResolver.for_cloud("azure", dbutils=fake_dbutils)
    assert azure_resolver.kwargs["azure_client_id"] == "client"

    aws_resolver = cc.DatabricksSecretResolver.for_cloud("aws", dbutils=fake_dbutils)
    assert isinstance(aws_resolver, FakeResolver)
    assert cc.os.environ["AWS_ACCESS_KEY_ID"] == "aws-key"

    gcp_resolver = cc.DatabricksSecretResolver.for_cloud("gcp", dbutils=fake_dbutils)
    assert isinstance(gcp_resolver, FakeResolver)
    assert cc.os.environ["GOOGLE_CLOUD_PROJECT"] == "project-1"

    with pytest.raises(ValueError, match="Unknown cloud"):
        cc.DatabricksSecretResolver.for_cloud("unknown", dbutils=fake_dbutils)

    fake_ipython = types.ModuleType("IPython")
    fake_ipython.get_ipython = lambda: types.SimpleNamespace(user_ns={"dbutils": fake_dbutils})
    monkeypatch.setitem(sys.modules, "IPython", fake_ipython)
    assert cc._get_dbutils() is fake_dbutils

    monkeypatch.setitem(sys.modules, "IPython", types.ModuleType("IPython"))
    sys.modules["IPython"].get_ipython = lambda: None
    with pytest.raises(RuntimeError, match="dbutils is not available"):
        cc._get_dbutils()

    assert isinstance(cc.from_databricks("azure", dbutils=fake_dbutils), FakeResolver)


def test_cloud_credential_default_azure_and_secret_edge_cases(monkeypatch):
    resolver = cc.CloudCredentialResolver(use_key_vault=True)

    fake_identity = types.ModuleType("azure.identity")

    class FakeDefaultAzureCredential:
        def get_token(self, scope):
            return types.SimpleNamespace(token=f"default::{scope}")

    fake_identity.DefaultAzureCredential = FakeDefaultAzureCredential
    monkeypatch.setitem(sys.modules, "azure.identity", fake_identity)

    azure_opts = resolver._resolve_azure_credentials({"account_name": "acct"})
    assert azure_opts["bearer_token"] == "default::https://storage.azure.com/.default"
    cached_opts = resolver._resolve_azure_credentials({"account_name": "acct"})
    assert cached_opts["bearer_token"] == azure_opts["bearer_token"]

    monkeypatch.delenv("AZURE_KEY_VAULT_URL", raising=False)
    assert resolver._get_azure_key_vault_secret("db-password") is None

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda service, region_name=None: types.SimpleNamespace(
        get_secret_value=lambda SecretId: {"SecretBinary": base64.b64encode(b"aws-binary-secret")}
    )
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    assert resolver._get_aws_secret("binary-secret", region="us-east-1") == "aws-binary-secret"

    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    assert resolver._get_gcp_secret("api-key") is None
