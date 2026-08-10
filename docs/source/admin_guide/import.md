(AdminImport)=
# Import and Export

For a user-perspective on this feature, go to [the user guide section on this](UserImport). For developer's notes no this, go to [the developer guide section on this](DeveloperImport).

Import and export via the web user interface need some configuration, primarily for the use case "export or import huge zip file", in order to prevent a smaller-scale directory like `/tmp/` from being flooded with huge amounts of data.

To configure the webserver to *accept imports and exports*, you have to specify an environment variable. The reason this feature isn't enabled by default is because uploading and extracting arbitrary .zip files is a security risk. 

Whether export and import are enabled depends on the `AMARCORD_IMPORT_EXPORT_SETTINGS` environment variable, which needs to start with `export:` and can then contain `|` separated `key=value` pairs. Currently, there are two keys that you can specify: `export-path` and `import-path`. A sample settings string would look like:

```
export:import-path=/opt/imports|export-path=/opt/exports
```

The `import-path` is used to extract the .zip file contents into. If it's a huge zip file, the directory has to hold that amount of data, too. The `export-path` is used to store the resulting .zip files of export jobs.

If you then start the web server, the export daemon will be started.
