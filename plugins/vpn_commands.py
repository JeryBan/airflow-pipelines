from airflow.cli.cli_config import ActionCommand, Arg, GroupCommand
from airflow.plugins_manager import AirflowPlugin


def open_openvpn(**kwargs):
    print('Open synelixis vpn')


def close_openvpn():
    print('Closing VPN connections...')


OPENVPN_ACTION_COMMANDS = (
    ActionCommand(
        name="open",
        help="open vpn",
        func=open_openvpn,
        args=(
            Arg(('-f', '--config-filepath'), type=str, help='Path to config file'),
        )
    ),
    ActionCommand(
        name='close',
        help="close vpn",
        func=close_openvpn,
        args=()
    )
)

openvpn_commands = GroupCommand(
        name='openvpn',
        help="openvpn connections",
        subcommands=OPENVPN_ACTION_COMMANDS
    )


class CloseVpnPlugin(AirflowPlugin):
    name = "vpn_plugin"

    # @classmethod
    # def on_load(cls, *args, **kwargs):
    #     """This method is called once when the plugin is loaded."""
    #     from airflow.cli.cli_parser import get_parser
    #
    #     # Get the root parser
    #     parser = get_parser()
    #     subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    #
    #     for command in openvpn_commands:
    #         subparsers.add_parser(
    #             name=command.name,
    #             help=command.help
    #         )
