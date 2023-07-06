from ipykernel.kernelbase import Kernel
from tableauhyperapi import HyperProcess, Connection, Telemetry, HyperException
from tabulate import tabulate
import time
import json
from datetime import datetime
import shlex


class HyperKernel(Kernel):
    implementation = 'Hyper'
    implementation_version = '0.0'
    language = 'sql'
    language_version = '0.0'
    language_info = {
        'name': 'sql',
        'mimetype': 'text/sql',
        'file_extension': '.sql',
    }
    banner = "Hyper 🚀 - Your friendly neighborhood SQL database.\n" +\
             "Type '\\?' for help."

    def __init__(self, *args, **kwargs):
        super(HyperKernel, self).__init__(*args, **kwargs)

        self._hyper_process = HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'jupyter_sql_kernel')
        self._connection = Connection(self._hyper_process.endpoint)
        self._output_func = self._display_output

    def do_shutdown(self, restart):
        self._connection.close()
        self._hyper_process.close()
        return {'status': 'ok', 'restart': restart}

    def _success_response(self, payloads=[]):
        return {
                'status': 'ok',
                # The base class increments the execution count for us already
                'execution_count': self.execution_count,
                'payload': payloads,
                'user_expressions': {},
               }

    def _error_response(self, ename, evalue, traceback):
        # Format & send the error message
        error_response = {
           'ename': ename,
           'evalue': evalue,
           'traceback': traceback
        }
        self.send_response(self.iopub_socket, 'error', error_response)
        error_response['status'] = 'error'
        error_response['execution_count'] = self.execution_count
        return error_response

    def _send_text(self, txt):
        self.send_response(self.iopub_socket, 'display_data', {'data': {'text/plain': txt}, 'metadata': {}})

    def _format_hyper_error(self, e):
        formatted = f"Error:\n{e.main_message}"
        if e.hint:
            formatted += f"HINT: {e.hint}"
        return formatted

    def _display_output(self, sql_result, silent):
        if not silent:
            column_names = [c.name for c in sql_result.schema.columns]
            result = list(sql_result)
            if column_names or result:
                response_data = {
                    'text/plain': tabulate(result, headers=column_names),
                    'text/html': tabulate(result, headers=column_names, tablefmt='html'),
                }
                # Integration with the "@tableau/query-graphs-jupyterlab-extension" extension for plan rendering in JupyterLab
                if column_names == ["plan"]:
                    try:
                        response_data['application/vnd.tableau.hyper-queryplan'] = json.loads("".join(row[0] for row in result))
                    except json.JSONDecodeError as e:
                        pass
                # Support for "Vega output" form Hyper.
                # In case the user is skilled enough to write a SQL query which outputs a Vega visualizations, go ahead and display the visualization in JupyterLab.
                if len(column_names) == 1 and len(result) == 1 and isinstance(result[0][0], str):
                    try:
                        parsed = json.loads(result[0][0])
                        if isinstance(parsed, dict):
                            if parsed.get("$schema", "").startswith('https://vega.github.io/schema/vega/'):
                                response_data['application/vnd.vega.v5+json'] = parsed
                                del response_data['text/html']
                            if parsed.get("$schema", "").startswith('https://vega.github.io/schema/vega-lite/'):
                                response_data['application/vnd.vegalite.v3+json'] = parsed
                                del response_data['text/html']
                    except json.JSONDecodeError as e:
                        pass
                self.send_response(self.iopub_socket, 'display_data', {'source': 'sql', 'data': response_data, 'metadata': {}})

    def _create_file_output_func(self, filename):
        def _file_output(self, sql_result, silent):
            with open(filename, "a") as f:
                column_names = [c.name for c in sql_result.schema.columns]
                result = list(sql_result)
                f.write(tabulate(result, headers=column_names))
                f.write("\n")
        return _file_output.__get__(self, HyperKernel)

    def _discard_output(self, sql_result, silent):
        if sql_result is not None and sql_result.schema is not None:
            # We still want to fetch the whole result (to not screw up timing measurements)
            for i in sql_result:
                pass

    def execute_sql(self, code, silent):
        "Execute a SQL query and display the results to the user"
        start_time = time.perf_counter()
        try:
            with self._connection.execute_query(code) as sql_result:
                self._output_func(sql_result, silent)
        except HyperException as e:
            # Format & send the error message
            return self._error_response(str("HyperException"), str(e.args[0]), [self._format_hyper_error(e)])

        end_time = time.perf_counter()
        elapsed = end_time - start_time
        self._send_text('{:.3f}s elapsed'.format(elapsed))

        return self._success_response()

    def _command_input_sql(self, args):
        """
        Read SQL query from a file and execute it
        """
        if len(args) != 1:
            return self._error_response("InvalidClientCommandArguments", repr(args), ["Unexpected number of arguments"])
        filename = args[0]
        try:
            with open(filename) as f:
                file_content = f.read()
        except:
            return self._error_response("IOError", repr(args), [f"Unable to read file '{filename}'"])
        self.execute_sql(file_content, silent=False)

    def _command_redirect_output(self, args):
        """
        Redirect output into a file
        """
        if len(args) > 1:
            return self._error_response("InvalidClientCommandArguments", repr(args), ["Unexpected number of arguments"])
        if len(args) == 0:
            self._output_func = self._display_output
        elif args[0] == "-":
            self._output_func = self._discard_output
        else:
            filename = args[0]
            # Truncate the file & create if it does not exist
            try:
                with open(filename, "w"):
                    pass
            except:
                return self._error_response("IOError", repr(args), [f"Unable to read file '{filename}'"])
            self._output_func = self._create_file_output_func(filename)

    def _command_attach(self, args):
        """
        Open a Hyper file
        """
        if len(args) != 2:
            return self._error_response("InvalidClientCommandArguments", repr(args), ["Unexpected number of arguments"])
        database_path = args[0]
        alias = args[1]
        try:
            self._connection.catalog.attach_database(database_path, alias)
        except HyperException as e:
            # Format & send the error message
            return self._error_response(str("HyperException"), str(e.args[0]), [self._format_hyper_error(e)])

    def _command_detach(self, args):
        """
        Close a Hyper file
        """
        if len(args) != 1:
            return self._error_response("InvalidClientCommandArguments", repr(args), ["Unexpected number of arguments"])
        alias = args[0]
        try:
            self._connection.catalog.detach_database(alias)
        except HyperException as e:
            # Format & send the error message
            return self._error_response(str("HyperException"), str(e.args[0]), [self._format_hyper_error(e)])

    def _process_client_command(self, code, silent):
        "Execute a client command"

        commands = {
            "i": self._command_input_sql,
            "o": self._command_redirect_output,
            "attach": self._command_attach,
            "detach": self._command_detach,
        }

        # Tokenize command line
        code = code.lstrip()
        assert code[0] == '\\'
        code = code[1:]
        args = list(shlex.split(code, posix=True))
        cmd = args.pop(0)

        if cmd == "?" or cmd == "help":
            help_text = 'SQL command reference: https://tableau.github.io/hyper-db/docs/sql/command/\n'
            help_text += 'Additional client-side commands:\n'
            help_text += tabulate((["\\" + c[0], c[1].__doc__] for c in commands.items()), tablefmt='plain')
            help_text += '\n'
            help_text += 'Parameters are parsed in POSIX shell manner.\n'
            self._send_text(help_text)
            return self._success_response()

        if cmd not in commands:
            return self._error_response("UnknownClientCommand", cmd, [f"Unknown client command \{cmd}"])

        response = commands[cmd](args)

        return response if response is not None else self._success_response()

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if code.lstrip()[0] == '\\':
            return self._process_client_command(code, silent)
        else:
            return self.execute_sql(code, silent)
