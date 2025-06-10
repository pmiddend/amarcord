# User Guide

## Introduction

This document will assume you're not a programmer, but a regular user of AMARCORD.

## Geometries

### Intro

To run indexing jobs, you need to provide a *geometry*, which defines *where* the detector is and how the detector pixels are *located* in relation to each other. Geometries are objects in AMARCORD that can be created, updated, deleted and simply enumerated in the user interface in the menu under "Library → Geometries".

### Managing geometries

```{figure} menu-libraries.png

How to get to the overview
```

What you see is a list of currently available geometries:

```{figure} geometry-overview.png
Overview of all geometries (just one) of a sample beamtime.
```

Just as with chemicals, you can add a new geometry from scratch or copy one from a prior beam time. Below that is a list of all current geometries.

Let's look at what happens when you press "Add geometry". You will see a form such as this:

```{figure} add-geometry.png
Clicking "Add geometry" and entering some data.
```

As you can see, only two things are needed when adding a geometry. First is a name, which *must be unique inside this beam time*, and which will appear in drop-down menus for indexing jobs, as well as for finished indexing results.

Below that is the geometry content. This is simply a long piece of text, corresponding to the [CrystFEL geometry](https://gitlab.desy.de/thomas.white/crystfel/-/blob/master/doc/man/crystfel_geometry.5.md) file format. What you see on the screenshot is, of course, not a complete geometry, although `clen ...` is a directive indicating the *camera length*, sometimes also called detector distance (the distance from the detector to the sample).

Of note here is the ability to use *placeholders* in the geometry content. These placeholders are in the so-called [mustache](https://mustache.github.io/) syntax, which you don't *really* have to learn. Just remember that you can use terms like `{{attributo_name}}` inside the geometry, which will be replaced by the actual attributo value for the run that we are indexing with. If you have a run table like this:

<table>
<tr>
<th>Run ID</th>
<th>Detector Distance</th>
<th>Energy</th>
<th>...</th>
</tr>
<tr>
<td>1</td>
<td>200.0</td>
<td>14.000eV</td>
<td>...</td>
</tr>
<tr>
<td>2</td>
<td>201.0</td>
<td>14.500eV</td>
<td>...</td>
</tr>
<tr>
<td>3</td>
<td>205.0</td>
<td>14.000eV</td>
<td>...</td>
</tr>
</table>

And you start an indexing job with the geometry above, we would get:

```
clen 200.0

more stuff here
```

as the geometry for run 1, for example.

Note that *editing* and *deleting* geometries is only possible if it is not in use in an indexing job. Once it's used, it stays.

### Using geometries

Geometries are needed during indexing, so if you start a new indexing job, you will see a drop-down menu with the available geometries:

```{figure} geometry-selection.png
Upon starting a new indexing job, we're greeted by this drop-down. Note that here, `geometry-v3.geom` is a geometry we uploaded manually, whereas the other ones are *generated* (see below).
```

Just select a geometry and submit the job. It will start an indexing job for each run, and use an adapted geometry for each one.


