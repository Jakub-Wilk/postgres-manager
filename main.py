#!/usr/bin/env python3
"""
PostgreSQL Database Manager with NiceGUI
Provides a web interface for dumping and restoring PostgreSQL databases.
"""

import asyncio
import os
import tomllib
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from nicegui import ui
from nicegui.element import Element


class PostgresManager:
    def __init__(self):
        self.config_path = Path("config.toml")
        self.connections = {}
        self.load_config()

        # UI state
        self.selected_connection = None
        self.dump_name_input = None
        self.restore_dropdown = None
        self.clean_db_checkbox = None
        self.status_label = None
        self.status_footer = None
        self.loading_overlay: Element | None = None

    def load_config(self):
        """Load database connections from config.toml"""
        try:
            with open(self.config_path, "rb") as f:
                config = tomllib.load(f)
                self.connections = config.get("connections", {})
        except FileNotFoundError:
            ui.notify("config.toml not found", type="negative")
            self.connections = {}
        except Exception as e:
            ui.notify(f"Error loading config: {e}", type="negative")
            self.connections = {}

    def get_connection_names(self) -> List[str]:
        """Get list of connection names"""
        return list(self.connections.keys())

    def is_restore_prevented(self, connection_name: str) -> bool:
        """Check if restore is prevented for a connection"""
        if connection_name not in self.connections:
            return False
        return self.connections[connection_name].get("prevent_restore", False)

    def reset_status_bar(self):
        """Reset status bar to normal state"""
        if self.status_label:
            self.status_label.text = "Ready"
        if self.status_footer:
            self.status_footer.classes(replace="p-4")

    def show_loading_overlay(self):
        """Show loading overlay with spinner"""
        if self.loading_overlay:
            self.loading_overlay.set_visibility(True)

    def hide_loading_overlay(self):
        """Hide loading overlay"""
        if self.loading_overlay:
            self.loading_overlay.set_visibility(False)

    def refresh_dump_name(self):
        """Refresh dump name with current timestamp"""
        if self.dump_name_input and self.selected_connection:
            self.dump_name_input.value = self.generate_dump_name(
                self.selected_connection
            )

    def get_dump_files(self, connection_name: str) -> List[str]:
        """Get list of dump files for a connection"""
        if connection_name not in self.connections:
            return []

        dump_path = Path(
            self.connections[connection_name].get("dump_path", ".")
        ).expanduser()
        if not dump_path.exists():
            return []

        dump_files = []
        for file in dump_path.glob("*.dump"):
            dump_files.append(file.name)

        return sorted(dump_files, reverse=True)  # Most recent first

    def generate_dump_name(self, connection_name: str) -> str:
        """Generate default dump name"""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{connection_name}_dump_{timestamp}"

    async def dump_database(self, connection_name: str, dump_name: str):
        """Dump database using pg_dump"""
        if connection_name not in self.connections:
            ui.notify("Invalid connection", type="negative")
            return

        conn_config = self.connections[connection_name]
        dump_path = Path(conn_config.get("dump_path", ".")).expanduser()

        # Ensure dump directory exists
        dump_path.mkdir(parents=True, exist_ok=True)

        # Add .dump extension if not present
        if not dump_name.endswith(".dump"):
            dump_name += ".dump"

        dump_file = dump_path / dump_name

        # Build pg_dump command with verbose output
        cmd = [
            "pg_dump",
            "-h",
            conn_config.get("host", "localhost"),
            "-p",
            str(conn_config.get("port", 5432)),
            "-U",
            conn_config.get("user", "postgres"),
            "-d",
            conn_config.get("dbname"),
            "-Fc",  # Custom format
            "--verbose",  # Enable verbose output for progress tracking
            "-f",
            str(dump_file),
        ]

        # Set environment variables
        env = os.environ.copy()
        env["PGPASSWORD"] = conn_config.get("password", "")

        try:
            self.show_loading_overlay()
            self.status_label.text = (
                f"Preparing to dump database {conn_config.get('dbname')}..."
            )
            await asyncio.sleep(0.1)  # Allow UI to update

            # Start the process
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Monitor progress by reading stderr line by line (pg_dump outputs progress to stderr)
            stderr_output = []
            table_count = 0
            processed_items = 0

            async def read_stderr():
                nonlocal table_count, processed_items
                try:
                    while True:
                        line = await process.stderr.readline()
                        if not line:
                            break

                        line_str = line.decode().strip()
                        if line_str:  # Only process non-empty lines
                            stderr_output.append(line_str)

                            # Parse progress from pg_dump verbose output
                            if "dumping contents of table" in line_str.lower():
                                table_count += 1
                                # Extract table name for data dumping
                                parts = line_str.split('"')
                                table_name = parts[1] if len(parts) > 1 else "unknown"
                                self.status_label.text = f"Dumping database... [Exporting data from {table_name}] ({table_count} tables)"
                                await asyncio.sleep(0.01)
                            elif "processing item" in line_str.lower():
                                processed_items += 1
                                self.status_label.text = f"Dumping database... [Processing item {processed_items}]"
                                await asyncio.sleep(0.01)
                            elif "reading schemas" in line_str.lower():
                                self.status_label.text = (
                                    "Dumping database... [Reading database schema]"
                                )
                                await asyncio.sleep(0.01)
                            elif "reading extensions" in line_str.lower():
                                self.status_label.text = (
                                    "Dumping database... [Reading extensions]"
                                )
                                await asyncio.sleep(0.01)
                            elif "reading types" in line_str.lower():
                                self.status_label.text = (
                                    "Dumping database... [Reading custom types]"
                                )
                                await asyncio.sleep(0.01)
                            elif "reading user-defined tables" in line_str.lower():
                                self.status_label.text = (
                                    "Dumping database... [Reading table structures]"
                                )
                                await asyncio.sleep(0.01)
                            elif "reading indexes" in line_str.lower():
                                self.status_label.text = (
                                    "Dumping database... [Reading indexes]"
                                )
                                await asyncio.sleep(0.01)
                            elif "reading constraints" in line_str.lower():
                                self.status_label.text = (
                                    "Dumping database... [Reading constraints]"
                                )
                                await asyncio.sleep(0.01)
                except Exception as e:
                    print(f"Error reading stderr: {e}")

            # Start reading stderr in background
            stderr_task = asyncio.create_task(read_stderr())

            # Wait for process to complete (only read stdout, stderr is handled above)
            await process.stdout.read()  # Consume stdout to prevent blocking

            # Wait for process to finish
            await process.wait()

            # Wait for stderr reading to complete
            await stderr_task

            if process.returncode == 0:
                ui.notify(
                    f"Database dumped successfully to {dump_name}", type="positive"
                )
                self.status_label.text = f"Dump completed: {dump_name} - {table_count} tables exported, {processed_items} items processed"
                # Refresh dump list if in restore mode
                if hasattr(self, "restore_dropdown") and self.restore_dropdown:
                    self.refresh_restore_dropdown()
                # Refresh dump name for next dump
                self.refresh_dump_name()
            else:
                # Join stderr output for error logging
                full_error = "\n".join(stderr_output)
                print("Full dump error output:", full_error)
                ui.notify("Dump failed: Check console for details", type="negative")
                self.status_label.text = "Dump failed - check console for details"

            self.hide_loading_overlay()

        except Exception as e:
            ui.notify(f"Error during dump: {e}", type="negative")
            self.status_label.text = f"Error during dump: {e}"
            self.hide_loading_overlay()

    async def clean_database(self, connection_name: str):
        """Drop all tables in the database"""
        if connection_name not in self.connections:
            return False

        conn_config = self.connections[connection_name]

        try:
            # Connect to database
            conn_str = f"postgresql://{conn_config.get('user')}:{conn_config.get('password')}@{conn_config.get('host', 'localhost')}:{conn_config.get('port', 5432)}/{conn_config.get('dbname')}"

            async with await psycopg.AsyncConnection.connect(conn_str) as conn:
                async with conn.cursor() as cur:
                    self.status_label.text = "Scanning database tables..."
                    await asyncio.sleep(0.1)  # Allow UI to update

                    # Get all table names
                    await cur.execute("""
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = 'public'
                    """)
                    tables = await cur.fetchall()

                    if tables:
                        table_names = [table[0] for table in tables]
                        total_tables = len(table_names)

                        self.status_label.text = (
                            f"Found {total_tables} tables to drop..."
                        )
                        await asyncio.sleep(0.1)  # Allow UI to update

                        # Drop tables one by one to show progress
                        for i, table_name in enumerate(table_names, 1):
                            self.status_label.text = f"Dropping tables... [{i}/{total_tables}] - {table_name}"
                            await asyncio.sleep(0.05)  # Allow UI to update

                            try:
                                await cur.execute(
                                    f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
                                )
                                await conn.commit()
                            except Exception as e:
                                # Continue with other tables even if one fails
                                print(
                                    f"Warning: Failed to drop table {table_name}: {e}"
                                )

                        self.status_label.text = (
                            f"Successfully dropped {total_tables} tables"
                        )
                        ui.notify(f"Dropped {total_tables} tables", type="info")
                    else:
                        self.status_label.text = "No tables found to drop"

            return True

        except Exception as e:
            ui.notify(f"Error cleaning database: {e}", type="negative")
            self.status_label.text = f"Error cleaning database: {e}"
            return False

    async def restore_database(
        self, connection_name: str, dump_file: str, clean_db: bool
    ):
        """Restore database using pg_restore"""
        if connection_name not in self.connections:
            ui.notify("Invalid connection", type="negative")
            return

        conn_config = self.connections[connection_name]
        dump_path = Path(conn_config.get("dump_path", ".")).expanduser()
        dump_file_path = dump_path / dump_file

        if not dump_file_path.exists():
            ui.notify("Dump file not found", type="negative")
            return

        try:
            self.show_loading_overlay()
            self.status_label.text = (
                f"Preparing to restore database {conn_config.get('dbname')}..."
            )
            await asyncio.sleep(0.1)  # Allow UI to update

            # Clean database if requested
            if clean_db:
                self.status_label.text = "Cleaning database before restore..."
                await asyncio.sleep(0.1)  # Allow UI to update
                success = await self.clean_database(connection_name)
                if not success:
                    self.hide_loading_overlay()
                    return

            # Build pg_restore command with verbose output
            cmd = [
                "pg_restore",
                "-h",
                conn_config.get("host", "localhost"),
                "-p",
                str(conn_config.get("port", 5432)),
                "-U",
                conn_config.get("user", "postgres"),
                "-d",
                conn_config.get("dbname"),
                "--no-owner",
                "--no-privileges",
                "--verbose",  # Enable verbose output for progress tracking
                str(dump_file_path),
            ]

            # Set environment variables
            env = os.environ.copy()
            env["PGPASSWORD"] = conn_config.get("password", "")

            self.status_label.text = "Starting database restore..."
            await asyncio.sleep(0.1)  # Allow UI to update

            # Start the process
            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Monitor progress by reading stderr line by line (pg_restore outputs progress to stderr)
            stderr_output = []
            table_count = 0
            processed_items = 0

            async def read_stderr():
                nonlocal table_count, processed_items
                try:
                    while True:
                        line = await process.stderr.readline()
                        if not line:
                            break

                        line_str = line.decode().strip()
                        if line_str:  # Only process non-empty lines
                            stderr_output.append(line_str)

                            # Parse progress from pg_restore verbose output
                            if "processing item" in line_str.lower():
                                processed_items += 1
                                self.status_label.text = f"Restoring database... [Processing item {processed_items}]"
                                await asyncio.sleep(
                                    0.01
                                )  # Small delay to allow UI updates
                            elif "creating table" in line_str.lower():
                                table_count += 1
                                # Extract table name if possible
                                parts = line_str.split()
                                table_name = ""
                                if len(parts) > 2:
                                    table_name = f" - created {parts[-1]}"
                                self.status_label.text = f"Restoring database... [Created {table_count} tables{table_name}. Continuing...]"
                                await asyncio.sleep(0.01)
                            elif "restoring data for table" in line_str.lower():
                                # Extract table name for data restoration
                                parts = line_str.split('"')
                                table_name = parts[1] if len(parts) > 1 else "unknown"
                                self.status_label.text = f"Restoring database... [Loading data into {table_name}]"
                                await asyncio.sleep(0.01)
                            elif "creating index" in line_str.lower():
                                self.status_label.text = "Restoring database... [Creating indexes and constraints]"
                                await asyncio.sleep(0.01)
                except Exception as e:
                    print(f"Error reading stderr: {e}")

            # Start reading stderr in background
            stderr_task = asyncio.create_task(read_stderr())

            # Wait for process to complete (only read stdout, stderr is handled above)
            await process.stdout.read()  # Consume stdout to prevent blocking

            # Wait for process to finish
            await process.wait()

            # Wait for stderr reading to complete
            await stderr_task

            if process.returncode == 0:
                ui.notify(
                    f"Database restored successfully from {dump_file}", type="positive"
                )
                self.status_label.text = f"Restore completed from {dump_file} - {table_count} tables, {processed_items} items processed"
                # Reset clean checkbox
                if self.clean_db_checkbox:
                    self.clean_db_checkbox.value = False
            else:
                # Join stderr output for error logging
                full_error = "\n".join(stderr_output)
                print("Full restore error output:", full_error)
                ui.notify("Restore failed: Check console for details", type="negative")
                self.status_label.text = "Restore failed - check console for details"

            self.hide_loading_overlay()

        except Exception as e:
            ui.notify(f"Error during restore: {e}", type="negative")
            self.status_label.text = f"Error during restore: {e}"
            self.hide_loading_overlay()

    def refresh_restore_dropdown(self):
        """Refresh the restore dropdown with current dump files"""
        if self.restore_dropdown and self.selected_connection:
            dump_files = self.get_dump_files(self.selected_connection)
            self.restore_dropdown.options = dump_files
            if dump_files:
                self.restore_dropdown.value = dump_files[0]
            else:
                self.restore_dropdown.value = None


# Global manager instance
manager = PostgresManager()


def create_dump_ui():
    """Create the dump mode UI"""
    with ui.column().classes("w-full max-w-md"):
        ui.label("Dump Mode").classes("text-2xl font-bold mb-4")

        # Connection selection
        connection_select = ui.select(
            options=manager.get_connection_names(),
            label="Select Connection",
            value=manager.get_connection_names()[0]
            if manager.get_connection_names()
            else None,
        ).classes("w-full mb-4")

        # Dump name input
        dump_name_input = ui.input(
            label="Dump Name", placeholder="Enter dump name"
        ).classes("w-full mb-4")

        manager.dump_name_input = dump_name_input

        def update_dump_name():
            if connection_select.value:
                manager.selected_connection = connection_select.value
                dump_name_input.value = manager.generate_dump_name(
                    connection_select.value
                )

        connection_select.on("update:model-value", lambda: update_dump_name())

        # Initialize dump name
        update_dump_name()

        # Dump button
        async def do_dump():
            if not connection_select.value:
                ui.notify("Please select a connection", type="warning")
                return
            if not dump_name_input.value:
                ui.notify("Please enter a dump name", type="warning")
                return

            await manager.dump_database(connection_select.value, dump_name_input.value)

        ui.button("Create Dump", on_click=do_dump).classes(
            "w-full bg-blue-600 text-white"
        )


def create_restore_ui():
    """Create the restore mode UI"""
    with ui.column().classes("w-full max-w-md"):
        ui.label("Restore Mode").classes("text-2xl font-bold mb-4")

        # Connection selection
        connection_select = ui.select(
            options=manager.get_connection_names(),
            label="Select Connection",
            value=manager.get_connection_names()[0]
            if manager.get_connection_names()
            else None,
        ).classes("w-full mb-4")

        # Dump file selection
        restore_dropdown = ui.select(
            options=[], label="Select Dump File", value=None
        ).classes("w-full mb-4")

        manager.restore_dropdown = restore_dropdown

        # Clean DB checkbox
        clean_db_checkbox = ui.checkbox(
            "Clean DB (Drops all tables)", value=False
        ).classes("mb-4")
        manager.clean_db_checkbox = clean_db_checkbox

        # Restore button
        restore_button = ui.button("Restore Database").classes(
            "w-full bg-green-600 text-white"
        )

        def update_restore_ui():
            if connection_select.value:
                manager.selected_connection = connection_select.value

                # Check if restore is prevented for this connection
                if manager.is_restore_prevented(connection_select.value):
                    # Disable all restore controls
                    restore_dropdown.disable()
                    clean_db_checkbox.disable()
                    restore_button.disable()

                    # Clear dropdown and checkbox
                    restore_dropdown.options = []
                    restore_dropdown.value = None
                    clean_db_checkbox.value = False

                    # Update status to show error
                    if manager.status_label:
                        manager.status_label.text = (
                            "Restore disabled for this connection"
                        )
                    if manager.status_footer:
                        manager.status_footer.classes(
                            replace="p-4 bg-red-500 text-white"
                        )
                else:
                    # Enable all restore controls
                    restore_dropdown.enable()
                    clean_db_checkbox.enable()
                    restore_button.enable()

                    # Load dump files
                    dump_files = manager.get_dump_files(connection_select.value)
                    restore_dropdown.options = dump_files
                    restore_dropdown.value = dump_files[0] if dump_files else None

                    # Reset status
                    if manager.status_label:
                        manager.status_label.text = "Ready"
                    if manager.status_footer:
                        manager.status_footer.classes(replace="p-4")

        connection_select.on("update:model-value", lambda: update_restore_ui())

        # Initialize restore UI state
        update_restore_ui()

        # Restore button click handler
        async def do_restore():
            if not connection_select.value:
                ui.notify("Please select a connection", type="warning")
                return
            if manager.is_restore_prevented(connection_select.value):
                ui.notify("Restore is disabled for this connection", type="negative")
                return
            if not restore_dropdown.value:
                ui.notify("Please select a dump file", type="warning")
                return

            await manager.restore_database(
                connection_select.value, restore_dropdown.value, clean_db_checkbox.value
            )

        restore_button.on_click(do_restore)


@ui.page("/")
def main_page():
    """Main application page"""
    ui.page_title("PostgreSQL Manager")

    with ui.header().classes("bg-gray-800 text-white"):
        ui.label("PostgreSQL Database Manager").classes("text-xl font-bold")

    with ui.column().classes("w-full items-center p-8"):
        # Mode tabs
        with ui.tabs().classes("w-full max-w-md") as tabs:
            dump_tab = ui.tab("Dump")
            restore_tab = ui.tab("Restore")

        # Add tab change handler to reset status
        def on_tab_change():
            if tabs.value == dump_tab.label:
                manager.reset_status_bar()
                # Refresh dump name with current timestamp when switching to dump mode
                manager.refresh_dump_name()
            elif tabs.value == restore_tab.label:
                # When switching to restore tab, check current connection status
                if (
                    hasattr(manager, "selected_connection")
                    and manager.selected_connection
                ):
                    if manager.is_restore_prevented(manager.selected_connection):
                        if manager.status_label:
                            manager.status_label.text = (
                                "Restore disabled for this connection"
                            )
                        if manager.status_footer:
                            manager.status_footer.classes(
                                replace="p-4 bg-red-500 text-white"
                            )
                    else:
                        manager.reset_status_bar()
                else:
                    manager.reset_status_bar()

        tabs.on("update:model-value", lambda: on_tab_change())

        with ui.tab_panels(tabs, value=dump_tab).classes("w-full max-w-xl p-6"):
            with ui.tab_panel(dump_tab).classes("flex items-center"):
                create_dump_ui()

            with ui.tab_panel(restore_tab).classes("flex items-center"):
                create_restore_ui()

        # Open dump folder button
        def open_dump_folder():
            if manager.selected_connection:
                conn_config = manager.connections[manager.selected_connection]
                dump_path = Path(conn_config.get("dump_path", ".")).expanduser()

                # Ensure the directory exists
                dump_path.mkdir(parents=True, exist_ok=True)

                try:
                    import platform
                    import subprocess

                    def is_wsl():
                        """Check if running in WSL (Windows Subsystem for Linux)"""
                        try:
                            with open("/proc/version", "r") as f:
                                content = f.read().lower()
                                return "microsoft" in content or "wsl" in content
                        except Exception:
                            return False

                    def convert_wsl_path_to_windows(linux_path):
                        """Convert WSL Linux path to Windows path"""
                        try:
                            # Use wslpath command to convert Linux path to Windows path
                            result = subprocess.run(
                                ["wslpath", "-w", str(linux_path)],
                                capture_output=True,
                                text=True,
                                check=True,
                            )
                            return result.stdout.strip()
                        except Exception:
                            # Fallback: manual conversion for common cases
                            path_str = str(linux_path)
                            if path_str.startswith("/mnt/"):
                                # /mnt/c/Users/... -> C:\Users\...
                                parts = path_str.split("/")
                                if len(parts) >= 3:
                                    drive = parts[2].upper()
                                    rest = "/".join(parts[3:])
                                    return f"{drive}:\\{rest.replace('/', '\\')}"
                            elif path_str.startswith("/home/"):
                                # /home/user/... -> \\wsl$\Ubuntu\home\user\...
                                return f"\\\\wsl$\\Ubuntu{path_str.replace('/', '\\')}"
                            return path_str

                    # Determine how to open file manager based on OS and WSL
                    if is_wsl():
                        # Running in WSL - use Windows explorer with converted path
                        windows_path = convert_wsl_path_to_windows(dump_path)
                        # Note: explorer.exe often returns non-zero exit codes in WSL even on success
                        subprocess.run(["explorer.exe", windows_path])
                        ui.notify(
                            f"Opened dump folder in Windows Explorer: {windows_path}",
                            type="positive",
                        )
                    elif platform.system() == "Linux":
                        # Native Linux
                        subprocess.run(["xdg-open", str(dump_path)], check=True)
                        ui.notify(f"Opened dump folder: {dump_path}", type="positive")
                    elif platform.system() == "Darwin":  # macOS
                        subprocess.run(["open", str(dump_path)], check=True)
                        ui.notify(f"Opened dump folder: {dump_path}", type="positive")
                    elif platform.system() == "Windows":
                        # Native Windows
                        subprocess.run(["explorer", str(dump_path)], check=True)
                        ui.notify(f"Opened dump folder: {dump_path}", type="positive")
                    else:
                        ui.notify(
                            "Unable to open file manager on this platform",
                            type="warning",
                        )
                        return

                except subprocess.CalledProcessError as e:
                    ui.notify(f"Failed to open dump folder: {e}", type="negative")
                except Exception as e:
                    ui.notify(f"Error opening dump folder: {e}", type="negative")
            else:
                ui.notify("Please select a connection first", type="warning")

        ui.button("📁 Open Dump Folder", on_click=open_dump_folder).classes(
            "mt-4 bg-gray-600 hover:bg-gray-700 text-white px-6 py-2 rounded-lg transition-colors"
        )

    # Status bar
    status_footer = ui.footer().classes("p-4")
    with status_footer:
        manager.status_label = ui.label("Ready").classes("text-sm")

    # Store reference to footer for background color changes
    manager.status_footer = status_footer

    loading_overlay = ui.element("div").classes(
        "fixed inset-0 bg-black/80 flex items-center justify-center z-50"
    )

    loading_overlay.set_visibility(False)

    # Loading overlay (initially hidden)
    with loading_overlay:
        ui.spinner("dots", size="xl", color="white")

    # Store reference to loading overlay
    manager.loading_overlay = loading_overlay


def main():
    """Main entry point"""
    if not manager.connections:
        print("No database connections found in config.toml")
        print("Please add connections to config.toml in the format:")
        print("""
[connections.connection_name]
host = "localhost"
port = 5432
dbname = "database_name"
user = "username"
password = "password"
dump_path = "/path/to/dump/directory"
prevent_restore = true # optional
        """)
        return

    print(f"Loaded {len(manager.connections)} database connections")
    for name in manager.connections:
        print(f"  - {name}")

    ui.run(
        title="PostgreSQL Manager",
        dark=True,
        show=True,
        reload=False,
        port=8081,
    )


if __name__ in ["__main__", "__mp_main__"]:
    main()
