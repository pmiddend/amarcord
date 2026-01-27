# Timestamps

## Definitions and use-cases

There is a distinction between a *time zone* (like "Europe/Berlin") and a *time zone offset* (like +01:00). Due to daylight savings, the Europe/Berlin time zone has two different offsets, depending on whether it's summer time (+2 hours after UTC) or not (+1 hour after UTC).

As a possible future use-case (which has not happened yet, just important to mention) we might have an experiment at a different time-zone from the server where AMARCORD is running on. Some services will be running in the experiment time zone, and others locally. If the indexing daemon starts a job in Germany for an experiment going on in Australia, which time stamp does it send to the server? Or vice-versa, if the job is started in Australia for an experiment going on in Germany?

## Constraints

As of 2025, you cannot get a lot of time zone or offset information via Javascript in the browser. There might be Javascript or Elm libraries to circumvent it, but in principle, these are hacks. For example, it's possible to get the current time zone offset in the browser, and even a time zone name. But it's not possible to get, for a specific date and time in UTC, the "offsetted" date and time in the current time zone for this given date. If you've stored runs in the DB in Europe/Berlin during winter, and the frontend only receives UTC timestamps for these runs, it will show the dates and times as summer time if you're currently in summer time.

In addition, it's also not clear which time zone you want to see as a user. Say the experiment took place in Europe/Berlin, but you're in Australia. Do you want to see that the runs were taken at 3pm "experiment time", or 11pm of your local time?

Database-wise, we want to be as compatible as possible with different DBMS (Postgres, Sqlite, MySQL, ...). They have varying support for storing dates and times with time zones.

## Solution

- Timestamps of any sort are stored *without a time zone or an offset* in the database.
- The *server* has a defined time-zone, changeable via the `AMARCORD_TZ` environment variable. In the future, this could be replaced by a beamtime-specific time zone, so experiments at other facilities are displayed in the appropriate time zone.
- In the API, time stamps are always reported as two POSIX time-stamps (so numerically, not as ISO 8601 strings): one is UTC for reference, and one is the time stamp in the server's time zone, for display. This way, the front-end never needs to know its own time zone to display dates.
- In requests from the browser to the API, like creating a new beamtime, timestamps are still transferred as POSIX numbers, but will optionally be converted into the server's time zone. So if the client sends "2025-04-30 15:00", this will be taken as "3pm in Europe/Berlin" if that's what's in `AMARCORD_TZ`, not "3pm in UTC", which would be extremely confusing for the user. A *service* that can reliably create UTC time stamps can still send the time stamps in UTC though.

## Code

The Python date/time conversion functions are located in `amarcord/db/attributi.py`. `local_int_to_utc_datetime` will be used whenever a time-stamp from the frontend is given, so it'll use `AMARCORD_TZ` to convert it to UTC and store it in the DB. The inverse of this is `utc_datetime_to_local_int`, and there are tests in `tests/test_int_datetime_conversions.py` to test if they work.

In the API, when the client gets a reply with a time stamp in it, generally speaking, the client gets *two* time stamps from the server: a UTC one, and a "local" one. Local meaning "in the server's time zone (set with `AMARCORD_TZ`)". Similarly, in most requests *to* the server, the client sends either a UTC time stamp or a local one (sometimes differentiated with a boolean specifying if the given timestamp is given in UTC or not). 
