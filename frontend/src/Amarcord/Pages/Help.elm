module Amarcord.Pages.Help exposing (..)

import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (em_, h1_, h2_, h3_, h4_, img_,  p_, strongText)
import Amarcord.Route as Route exposing (makeLink)
import Html exposing (a, div, h5, img, li, ol, text, ul)
import Html.Attributes exposing (class, href, src)


view : Html.Html msg
view =
    div [ class "container" ]
        [ h1_ [ text "Welcome to AMARCORD!" ]
        , h2_ [ text "Overview" ]
        , p_ [ text "There are a few concepts you need to learn about to start working with AMARCORD. Check out the following diagram:" ]
        , img_ [ src "overview.svg" ]
        , p_
            [ text "As you can see, the concept of "
            , strongText "attributi"
            , text " is key to understanding how to customize AMARCORD. Let's explain this using a concrete example."
            ]
        , h2_ [ text "Toy Project: Screening Lysozyme at EuXFEL" ]
        , p_
            [ text "Let’s say we got some beam time at "
            , a [ href "https://www.xfel.eu/" ] [ text "European XFEL" ]
            , text " and want to screen some "
            , a [ href "https://en.wikipedia.org/wiki/Lysozyme" ] [ text "Lysozyme" ]
            , text " crystals (see image) that we create in our lab."
            , img [ class "float-end m-3", src "lyzo.jpg" ] []
            , text "There are five basics steps we need to take in order to set up the experiment:"
            ]
        , ol []
            [ li [] [ text "Create ", em_ [ text "sample attributi" ], text ". In this step, you decide which properties you want to keep track of with your sample(s)." ]
            , li [] [ text "Create ", em_ [ text "samples." ] ]
            , li [] [ text "Create ", em_ [ text "run attributi" ], text ". Here, you decide what you want to track during the experiment." ]
            , li [] [ text "Create ", em_ [ text "experiment types" ], text ". These determine which attributes of a run are used to group runs together for analysis." ]
            , li [] [ text "Create ", em_ [ text "data sets" ], text ". Create actual groups of runs by specifying their attributi values." ]
            ]
        , p_ [ text "The following diagram shows these steps. Note that Sample setup and Data Set creation are independent here.", text " It’s when you actually start the experiment that everything comes together." ]
        , img [ src "workflow.png" ] []
        , h3_ [ text "Sample attributi" ]
        , p_ [ text "Attributi (that's Italian, not a typo) are, well, attributes! They can be attached to Runs or Samples, and it’s completely up to you to choose which attributes to create. It depends on what you want to store for each sample or each run." ]
        , p_ [ text "Let’s define some sample attributi first. Our samples are all Lysozyme crystals — but they were created with different PEG concentrations. We want to screen and analyze them separately. So it makes sense to create a new attributo called “PEG concentration”. We first navigate to “Admin”, then “Attributi” (or just click ", a [ href (makeLink Route.Attributi) ] [ text "here" ], text ")." ]
        , div [ class "alert alert-primary" ]
            [ h5 [ class "alert-heading" ] [ icon { name = "info-circle" }, text " Am I an admin now‽" ]
            , p_ [ text "Well, kind of! Reading this, you're responsible for an experiment. And in that, you decide certain boundary conditions. In this case, what people should note when adding samples to the database." ]
            ]
        , p_ [ text "Press the button to add a new attributo at the top. You're greeted with a rather extensive form:" ]
        , img [ class "shadow m-3", src "add-attributo.png" ] []
        , p_ [ text "However, the only thing you have to enter is the ", em_ [ text "name" ], text " of the attributo. If you do that and press the add button, you've created the attributo. However, let's use most of what AMARCORD provides in order to be more specific with the attributo." ]
        , ul []
            [ li [] [ text "First, enter “PEG concentration” into the name field of the form. The name can contain arbitrary characters, but has to be unique among all attributi." ]
            , li [] [ text "Enter a description if you want to remind the sample people (or yourself) when they enter samples of what this quantity means." ]
            , li [] [ text "Change the type to be “number”, since the concentration is a number." ]
            , li [] [ text "Some new form elements pop up. It's now asking you for a ", em_ [ text "range" ], text " and a suffix/unit. You can enter a valid range here, like [0,100], or leave the field blank. Same for the unit. Note that percent is not a proper unit, but a suffix." ]
            , li [] [ text "Press add. The attributo should appear in the list." ]
            ]
        , img [ class "shadow m-3", src "sample-attributo-created.png" ] []
        , h3_ [ text "Adding samples" ]
        , p_ [ text "Now let’s add some of our Lyso crystals into the database. To do that, go to “Library” and then “Samples” (or click ", a [ href (makeLink Route.Samples) ] [ text "here" ], text "). Then press “Add sample”. You’ll be greeted with another form:" ]
        , img [ class "shadow m-3", src "add-sample.png" ] []
        , p_ [ text "As you can see, our PEG concentration appears here. And as with the attributi, you have to at least enter a ", em_ [ text "name" ], text " for the sample. Everything else is optional. You can also upload files, like micrograph images or protocols here." ]
        , p_ [ text "Let's add two samples, \"Lyso1\" and \"Lyso2\" with different concentrations:" ]
        , img [ class "shadow m-3", src "samples-added.png" ] []
        , h3_ [ text "Runs" ]
        , p_ [ text "The PEG concentration is attributo for a sample. As you can tell from the attributo table, apart from “started” and “stopped”, we don’t have any attributi for runs yet." ]
        , p_ [ text "This means that we cannot associate a run to a sample yet. Which makes adding samples…pretty useless! " ]
        , p_ [ text "Let’s change this by adding a “sample” attributo to the runs. Again, go to the attributo screen, press “Add attributo”, enter “sample” for the name and use the attributo type “Sample”:" ]
        , img [ class "shadow m-3", src "add-sample-to-run.png" ] []
        , p_ [ text "Press “Add new attributo” and you’re good to go." ]
        , p_ [ text "Notice that there’s nothing stopping you from adding another attributo of type “Sample” to a run. That might be just what you need for your experiment, and AMARCORD is flexible enough to let you do that. 😌" ]
        , h4_ [ text "Adding runs" ]
        , p_ [ text "What about adding runs? Well, while you can do that from the GUI, that’s not (!) the preferred way to operate. AMARCORD is about automation, and pushing the current run from the device itself, automatically, is the way to go. There will be a tutorial about the API as well, but that’s a WIP. For now, let’s continue with the experiment." ]
        , p_ [ text "But how do we assign samples to runs now? It's very simple: go to “Overview”. Since we haven't started a run, here's how that looks:" ]
        , img [ class "shadow m-3", src "run-overview-1.png" ] []
        , p_ [ text "To the right, you see information about the ", em_ [ text "current" ], text " run. Since the only run attributo we've added is the sample attributo, that's what we get. It's a dropdown where you can select one of the samples, or explicitly set no sample at all." ]
        , p_ [ text "But what's this thing on the left? Experiment Type? Data sets? Let's explore!" ]
        , h3_ [ text "Experiment types and data sets" ]
        , p_ [ text "Planning an experiment usually involves not only thinking about ", em_ [ text "which" ], text " samples to screen, but also ", em_ [ text "how" ], text " to screen them. Maybe you're mixing the protein with a substrate and want to try different delay times. Maybe you want to try beamline setups. To express these different conditions, AMARCORD has the notion of \"Experiment Types\"." ]
        , p_ [ text "An ", em_ [ text "experiment type" ], text " is actually just a list of attributi names. A ", em_ [ text "data set" ], text " then, is an \"instance\" of an experiment type, meaning you assign concrete values to the attributi." ]
        , p_ [ text "The simplest example of an experiment type - and one that we can create right now - contains just the \"sample\" attributo we created. It might be boring, but it's not completely useless." ]
        , p_ [ text "To create the experiment type, go to \"Admin\" and then \"Experiment Types\" (or click ", a [ href (makeLink Route.ExperimentTypes) ] [ text "here" ], text ")." ]
        , img [ class "shadow m-3", src "new-experiment-type.png" ] []
        , p_ [ text "What you see here is very minimal (and a work in progress for now). Enter \"sample based\" as the experiment type name and \"sample\" as the attributi names and press \"Add type\". That's it." ]
        , p_ [ text "Now on to data sets. These reside in a different page, because they belong more to the \"data\" side than the \"setup\" side. Just like samples and sample attributi are not on the same page. To add two data sets for our two Lysozyme samples, go to \"Library\" and click \"Data sets\" (or click ", a [ href (makeLink Route.DataSets) ] [ text "here" ], text ")." ]
        , img [ class "shadow m-3", src "new-data-set.png" ] []
        , p_ [ text "The screenshot gives you an indication of what do to. But what do these data sets bring to the table? Well, they allow you to ", em_ [ text "group" ], text " your runs, and analyze these groups, and allow AMARCORD to show you the grouped information." ]
        , p_ [ text "For instance, suppose you are using ", a [ href "https://www.ondamonitor.com/html/index.html" ] [ text "Om" ], text " to monitor your crystallography experiment in real-time. Om gives you information about the number of ", em_ [ text "frames" ], text " and the number of ", em_ [ text "hits" ], text ". But Om doesn't really know about runs and samples. So if you want to know how many hits a sample has in total, you need to group that information. And AMARCORD can actually do that for you! All you have to do is create a data set for each sample (and start the Om ingest daemon for AMARCORD) and then check out the run overview:" ]
        , img [ class "shadow m-3", src "hit-stats.png" ] []
        , p_ [ text "As you can see, we're currently in the \"sample-based\" experiment that we created. AMARCORD has detected a data set for the current sample and shows the number of frames, the number of runs matching that data set and cumulated hits/frames." ]
        ]
