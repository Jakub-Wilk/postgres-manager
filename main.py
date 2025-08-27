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
        if connection_name not in self.connections:
            return ""

        db_name = self.connections[connection_name].get("dbname", "db")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{db_name}_dump_{timestamp}"

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

        # Build pg_dump command
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
            "-f",
            str(dump_file),
        ]

        # Set environment variables
        env = os.environ.copy()
        env["PGPASSWORD"] = conn_config.get("password", "")

        try:
            self.status_label.text = f"Dumping database {conn_config.get('dbname')}..."

            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                ui.notify(
                    f"Database dumped successfully to {dump_name}", type="positive"
                )
                self.status_label.text = f"Dump completed: {dump_name}"
                # Refresh dump list if in restore mode
                if hasattr(self, "restore_dropdown") and self.restore_dropdown:
                    self.refresh_restore_dropdown()
            else:
                error_msg = stderr.decode() if stderr else "Unknown error"
                ui.notify(f"Dump failed: {error_msg}", type="negative")
                self.status_label.text = f"Dump failed: {error_msg}"

        except Exception as e:
            ui.notify(f"Error during dump: {e}", type="negative")
            self.status_label.text = f"Error: {e}"

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
                    # Get all table names
                    await cur.execute("""
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = 'public'
                    """)
                    tables = await cur.fetchall()

                    if tables:
                        # Drop all tables
                        table_names = [table[0] for table in tables]
                        tables_str = ", ".join(f'"{table}"' for table in table_names)
                        await cur.execute(f"DROP TABLE IF EXISTS {tables_str} CASCADE")
                        await conn.commit()

                        ui.notify(f"Dropped {len(tables)} tables", type="info")

            return True

        except Exception as e:
            ui.notify(f"Error cleaning database: {e}", type="negative")
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
            self.status_label.text = (
                f"Restoring database {conn_config.get('dbname')}..."
            )

            # Clean database if requested
            if clean_db:
                self.status_label.text = "Cleaning database..."
                success = await self.clean_database(connection_name)
                if not success:
                    return

            # Build pg_restore command
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
                str(dump_file_path),
            ]

            # Set environment variables
            env = os.environ.copy()
            env["PGPASSWORD"] = conn_config.get("password", "")

            process = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                ui.notify(
                    f"Database restored successfully from {dump_file}", type="positive"
                )
                self.status_label.text = f"Restore completed from {dump_file}"
                # Reset clean checkbox
                if self.clean_db_checkbox:
                    self.clean_db_checkbox.value = False
            else:
                error_msg = stderr.decode() if stderr else "Unknown error"
                print(error_msg)
                ui.notify("Restore failed: Error logged", type="negative")
                self.status_label.text = "Restore failed"

        except Exception as e:
            ui.notify(f"Error during restore: {e}", type="negative")
            self.status_label.text = f"Error: {e}"

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
        clean_db_checkbox = ui.checkbox("Clean DB", value=False).classes("mb-4")
        manager.clean_db_checkbox = clean_db_checkbox

        def update_restore_files():
            if connection_select.value:
                manager.selected_connection = connection_select.value
                dump_files = manager.get_dump_files(connection_select.value)
                restore_dropdown.options = dump_files
                restore_dropdown.value = dump_files[0] if dump_files else None

        connection_select.on("update:model-value", lambda: update_restore_files())

        # Initialize restore files
        update_restore_files()

        # Restore button
        async def do_restore():
            if not connection_select.value:
                ui.notify("Please select a connection", type="warning")
                return
            if not restore_dropdown.value:
                ui.notify("Please select a dump file", type="warning")
                return

            await manager.restore_database(
                connection_select.value, restore_dropdown.value, clean_db_checkbox.value
            )

        ui.button("Restore Database", on_click=do_restore).classes(
            "w-full bg-green-600 text-white"
        )


@ui.page("/")
def main_page():
    """Main application page"""
    ui.page_title("PostgreSQL Manager")

    with ui.header().classes("bg-gray-800 text-white"):
        ui.label("PostgreSQL Database Manager").classes("text-xl font-bold")

    with ui.row().classes("w-full justify-center p-8"):
        # Mode tabs
        with ui.tabs().classes("w-full max-w-4xl") as tabs:
            dump_tab = ui.tab("Dump")
            restore_tab = ui.tab("Restore")

        with ui.tab_panels(tabs, value=dump_tab).classes("w-full max-w-4xl"):
            with ui.tab_panel(dump_tab):
                create_dump_ui()

            with ui.tab_panel(restore_tab):
                create_restore_ui()

    # Status bar
    with ui.footer().classes("p-4"):
        manager.status_label = ui.label("Ready").classes("text-sm")


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


if __name__ == "__main__":
    main()
