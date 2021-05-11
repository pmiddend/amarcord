from typing import Any
from typing import Dict
from typing import Optional
import copy
import datetime
import pandas as pd
import tabulate
from bs4 import BeautifulSoup

from amarcord.db.db import DB
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext


def ELOGParser(soup) -> Dict[int, Any]:
    data_cell = soup.find_all("td", recursive=True)
    message = {}

    message_definition = {
        "status": None,
        "time": None,
        "author": None,
        "tag": None,
        "title": None,
        "content": None,
        "type": None,
    }
    header_counter = 0
    last_index = 0

    for td in data_cell:
        try:
            href = "https://{}".format(td.find("a").attrs["href"].split("https://")[1])[
                :-1
            ]
        except AttributeError:
            pass

        if "class" in td.attrs:

            # header
            if td.attrs["class"][0] == "list1":
                index = int(td.a.attrs["href"].split()[0].split("/")[-1])

                if index != last_index:
                    last_index = index

                    message[index] = copy.deepcopy(message_definition)
                    header_counter = 0

                message[index][
                    list(message_definition.keys())[header_counter]
                ] = td.a.get_text(strip=True)
                header_counter += 1

            # content
            elif td.attrs["class"][0] == "messagelist":

                for ei in td.find_all():
                    if ei.name == "table":
                        df = pd.read_html(ei.prettify())
                        content = df[0]
                        content.columns = content.iloc[0].tolist()

                        message[index]["href"] = href
                        message[index]["type"] = ei.name
                        message[index]["content"] = content[1:]

    return message


def pprint(index: int, message: Dict[str, Any]):
    print("Message {} ({})".format(index, message["time"]))
    print("Author: {}".format(message["author"]))
    print("Tag: {}".format(message["tag"]))
    print("Title: {}".format(message["title"]))
    print(
        tabulate.tabulate(
            message["content"], headers="keys", tablefmt="simple", showindex="never",
        ),
        "\n",
    )


if __name__ == "__main__":
    from tap import Tap

    class Parser(Tap):
        """Parse the ELOG"""

        db_connection_url: Optional[
            str
        ] = None  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
        html: str  # File containing the html source (view-source:https://in.xfel.emessage["time"]u/elog/SPB-SFX+Proposal+2696/page?mode=full)
        elog_entry: str = "all"  # Message index or 'all' to get an overview
        run_header: str = "run"  # Column containg the run index
        ingest_header: str = "comment"  # Column containing the string to ingest
        run_index: str = "all"  # Extract this run, or 'all'
        echo: bool = False  # Echo AMARCORD commands

    args = Parser(underscores_to_dashes=True).parse_args()

    try:
        with open(args.html, "rb") as fh:
            soup = BeautifulSoup(fh, "html.parser")

    except FileNotFoundError:
        raise FileNotFoundError("{} is not accessible.".format(args.html))

    title = soup.find("title").get_text()
    proposal_id = int(title.split()[-1])
    print("{}\n".format(title))

    # parse the html
    message = ELOGParser(soup)
    missing_run = []

    if args.elog_entry == "all":
        for ki, vi in message.items():
            if vi["type"] == "table":
                pprint(ki, vi)

    else:
        table = message[int(args.elog_entry)]
        run_identifier = table["content"][args.run_header]
        comment = table["content"][args.ingest_header]

        run, run_lut = [], []
        for position in range(1, len(run_identifier) + 1):
            if (
                pd.isna(run_identifier[position])
                or pd.isna(comment[position])
                or run_identifier[position] in ["-", "*"]
            ):
                continue

            if run_identifier[position].replace(" ", "").find("-") > -1:
                inner = list(
                    range(
                        int(run_identifier[position].split("-")[0]),
                        int(run_identifier[position].split("-")[1]) + 1,
                    )
                )
            else:
                inner = [int(run_identifier[position])]

            for value in inner:
                run.append(value)
                run_lut.append(position)

        for ri in [args.run_index] if args.run_index != "all" else run:
            index = 0

            for index in range(0, len(run)):
                if int(ri) == int(run[index]):
                    index = run_lut[index]

                    break

            print("INFO. Run {}: {}".format(ri, comment[index],))

            if args.db_connection_url is not None:
                dbcontext = DBContext(args.db_connection_url, echo=args.echo)

                tables = create_tables(dbcontext)
                if args.db_connection_url.startswith("sqlite://"):
                    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
                db = DB(dbcontext, tables)

                with db.connect() as conn:

                    # get all the runs collected
                    run_id_list = db.retrieve_run_ids(conn, proposal_id=proposal_id)

                    if int(ri) in run_id_list:
                        comment_content = "|ELOG-{}/{}| {}".format(
                            proposal_id, args.elog_entry, comment[index]
                        )

                        # is there a comment already?
                        comment_is_there = False
                        run_data = db.retrieve_run(conn, proposal_id, ri)

                        for ci in run_data.comments:
                            if ci.text == comment_content:
                                comment_is_there = True

                        # add the comment
                        if not comment_is_there:
                            db.add_comment(
                                conn,
                                ri,
                                message[int(args.elog_entry)]["author"],
                                comment_content,
                                time=datetime.datetime.strptime(
                                    message[int(args.elog_entry)]["time"],
                                    "%d %b %Y, %H:%M",
                                ),
                            )

                    else:
                        print("WARN. Run {} not found".format(ri))
                        missing_run.append(int(ri))

        print("\n", " ".join(["*" for _ in range(50)]))
        pprint(index, table)
