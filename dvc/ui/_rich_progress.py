from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


class RichProgress(Progress):
    def get_renderables(self):
        for task in self.tasks:
            if task.fields.get("progress_type") == "summary":
                self.columns = (
                    TextColumn(
                        "[magenta]{task.description}[bold green]",
                        justify="right",
                    ),
                    TextColumn(
                        "([green]{task.completed}/"
                        "{task.total}{task.fields[unit]})",
                        justify="right",
                    ),
                    TimeElapsedColumn(),
                )
            else:
                self.columns = (
                    TextColumn("[blue]{task.description}", justify="right"),
                    BarColumn(),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TextColumn("eta"),
                    TimeRemainingColumn(),
                )
            yield self.make_tasks_table([task])
