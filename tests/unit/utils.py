"""Unit test utility functions.

Note that all imports should be inside the functions to avoid import/mocking
issues.
"""
import os
from unittest import TestCase, mock

from dbt.config.project import PartialProject
from hologram import ValidationError


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


class Obj:
    which = "blah"
    single_threaded = False


def mock_connection(name):
    conn = mock.MagicMock()
    conn.name = name
    return conn


def profile_from_dict(profile, profile_name, cli_vars="{}"):
    from dbt.config import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars
    from dbt.context.base import generate_base_context

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = ProfileRenderer(cli_vars)
    return Profile.from_raw_profile_info(
        profile,
        profile_name,
        renderer,
    )


def project_from_dict(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from dbt.config.renderer import DbtProjectYamlRenderer
    from dbt.config.utils import parse_cli_vars

    if not isinstance(cli_vars, dict):
        cli_vars = parse_cli_vars(cli_vars)

    renderer = DbtProjectYamlRenderer(profile, cli_vars)

    project_root = project.pop("project-root", os.getcwd())

    partial = PartialProject.from_dicts(
        project_root=project_root,
        project_dict=project,
        packages_dict=packages,
        selectors_dict=selectors,
    )
    return partial.render(renderer)


def config_from_parts_or_dicts(project, profile, packages=None, selectors=None, cli_vars="{}"):
    from copy import deepcopy

    from dbt.config import Profile, Project, RuntimeConfig

    if isinstance(project, Project):
        profile_name = project.profile_name
    else:
        profile_name = project.get("profile")

    if not isinstance(profile, Profile):
        profile = profile_from_dict(
            deepcopy(profile),
            profile_name,
            cli_vars,
        )

    if not isinstance(project, Project):
        project = project_from_dict(
            deepcopy(project),
            profile,
            packages,
            selectors,
            cli_vars,
        )

    args = Obj()
    args.vars = cli_vars
    args.profile_dir = "/dev/null"
    return RuntimeConfig.from_parts(project=project, profile=profile, args=args)


def inject_plugin(plugin):
    from dbt.adapters.factory import FACTORY

    key = plugin.adapter.type()
    FACTORY.plugins[key] = plugin


def inject_adapter(value, plugin):
    """Inject the given adapter into the adapter factory, so your hand-crafted
    artisanal adapter will be available from get_adapter() as if dbt loaded it.
    """
    inject_plugin(plugin)
    from dbt.adapters.factory import FACTORY

    key = value.type()
    FACTORY.adapters[key] = value


class ContractTestCase(TestCase):
    ContractType = None

    def setUp(self):
        self.maxDiff = None
        super().setUp()

    def assert_to_dict(self, obj, dct):
        self.assertEqual(obj.to_dict(), dct)

    def assert_from_dict(self, obj, dct, cls=None):
        if cls is None:
            cls = self.ContractType
        self.assertEqual(cls.from_dict(dct), obj)

    def assert_symmetric(self, obj, dct, cls=None):
        self.assert_to_dict(obj, dct)
        self.assert_from_dict(obj, dct, cls)

    def assert_fails_validation(self, dct, cls=None):
        if cls is None:
            cls = self.ContractType

        with self.assertRaises(ValidationError):
            cls.from_dict(dct)


def generate_name_macros(package):
    from dbt.contracts.graph.parsed import ParsedMacro
    from dbt.node_types import NodeType

    name_sql = {}
    for component in ("database", "schema", "alias"):
        if component == "alias":
            source = "node.name"
        else:
            source = f"target.{component}"
        name = f"generate_{component}_name"
        sql = f"{{% macro {name}(value, node) %}} {{% if value %}} {{{{ value }}}} {{% else %}} {{{{ {source} }}}} {{% endif %}} {{% endmacro %}}"
        name_sql[name] = sql

    all_sql = "\n".join(name_sql.values())
    for name, sql in name_sql.items():
        pm = ParsedMacro(
            name=name,
            resource_type=NodeType.Macro,
            unique_id=f"macro.{package}.{name}",
            package_name=package,
            original_file_path=normalize("macros/macro.sql"),
            root_path="./dbt_modules/root",
            path=normalize("macros/macro.sql"),
            raw_sql=all_sql,
            macro_sql=sql,
        )
        yield pm
