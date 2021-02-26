from PyQt5 import QtWidgets


def filter_query_help(parent: QtWidgets.QWidget) -> None:
    dialog = QtWidgets.QDialog(parent)
    dialog_layout = QtWidgets.QVBoxLayout()
    dialog.setLayout(dialog_layout)

    help_text = QtWidgets.QTextEdit()
    help_text.setReadOnly(True)
    help_text.setHtml(
        """
    <h1>Filter queries</h1>
    <h2>Description</h2>
    <p>
      A filter queries consist of a single line of text that contains <b>conditions</b>
      separated by <b>logical operators</b>.
    </p>
    <p>
      Supported logical operators are <i>and</i>, <i>or</i> and <i>not</i>. Conditions are of the form:
      <pre>field operator comparison</pre>
      Supported operators are <i>!=, =, &lt;, &lt;=, &gt;, &gt;=</i> and <i>has</i>. The latter can
      inversely be read as &quot;in&quot; and applies to strings and lists.
    </p>
    <h2>Special notes</h2>
    <ol>
    <li>White-space is skipped</li>
    <li>Strings are compared case-insensitively</li>
    <li>Spaces in strings are currently not supported</li>
    </ol>
    <h2>Examples</h2>
    <ul>
    <li>A simple comparison: <code>repetition_rate &gt; 3</code></li>
    <li>Comparison of two fields: <code>repetition_rate &gt; 3 and run_id &lt; 10</code></li>
    <li>Using <i>not</i>: <code>repetition_rate &gt; 3 and not run_id &lt; 10</code></li>
    <li>Using <i>has</i>: <code>tags has darks</code></li>
    </ul>
    """
    )
    help_text.setStyleSheet("background-color: transparent;")
    dialog_layout.addWidget(help_text)

    button_box = QtWidgets.QDialogButtonBox(  # type: ignore
        QtWidgets.QDialogButtonBox.Close
    )
    button_box.rejected.connect(dialog.reject)
    dialog_layout.addWidget(button_box)

    dialog.exec()
