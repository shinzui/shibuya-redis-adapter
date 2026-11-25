let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/1ed7527a9132dd3f4f2251c2ab7891fadb7006e8/package.dhall
        sha256:30f64b8cbeb90ab5afa76a523b05da707006bc31e886eec0aeb79e36f8b0afa1

let pinned =
      \(name : Text) ->
      \(constraint : Text) ->
        Schema.Dependency.WithAugmentation
          { name
          , extraDocs = [] : List Schema.DocRef.Type
          , localPathOverride = None Text
          , kind = None Schema.DependencyKind
          , source = Some Schema.DependencySource.Hackage
          , scope = Some Schema.DependencyScope.Regular
          , versionConstraint = Some constraint
          }

let testOnly =
      \(name : Text) ->
        Schema.Dependency.WithAugmentation
          { name
          , extraDocs = [] : List Schema.DocRef.Type
          , localPathOverride = None Text
          , kind = None Schema.DependencyKind
          , source = Some Schema.DependencySource.Hackage
          , scope = Some Schema.DependencyScope.Test
          , versionConstraint = None Text
          }

let pkgRef =
      \(namespace : Text) ->
      \(name : Text) ->
      \(key : Text) ->
        Schema.MoriRef::{
        , namespace
        , name
        , kind = Some Schema.MoriArtifactKind.Package
        , key = Some key
        }

let okfBinding =
      \(profileExport : Text) ->
        Some
          ( Schema.ProfileBinding.Published
              Schema.PinnedImport::{
              , publisher = "mori://shinzui/okf-profiles"
              , publisherRef = Some
                  Schema.MoriRef::{ namespace = "shinzui", name = "okf-profiles" }
              , export = Some profileExport
              , version = Some "v0.25.0"
              , pin = Some
                  "sha256:ec74cc6f7b8ee1a5af81ee7fc8804f35cf2c9ba4010671d9e881da72eb6719d3"
              }
          )

in  Schema.Project::{
    , project = Schema.ProjectIdentity::{
      , name = "shibuya-redis-adapter"
      , namespace = "shinzui"
      , stableId = Some "project_01m89cqhyhe1gvamp3410px5hs"
      , type = Schema.PackageType.Library
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Experimental
      , description = Some
          "Redis Streams adapter for the Shibuya queue processing framework"
      , domains = [ "concurrency", "queue-processing", "redis" ]
      , owners = [ "shinzui" ]
      }
    , repos =
      [ Schema.Repo::{
        , name = "shibuya-redis-adapter"
        , github = Some "shinzui/shibuya-redis-adapter"
        , localPath = Some "."
        }
      ]
    , packages =
      [ Schema.Package::{
        , name = "shibuya-redis-adapter"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , path = Some "."
        , description = Some
            "Redis Streams adapter: independent readers with checkpoints and consumer-group workers with pending-entry recovery"
        , dependencies =
          [ pinned "shinzui/shibuya:shibuya-core" "^>=0.13.0.1"
          , pinned "informatikr/hedis:hedis" "^>=0.16.4"
          , Schema.Dependency.ByName "effectful/effectful:effectful-core"
          , Schema.Dependency.ByName "composewell/streamly:streamly-core"
          , Schema.Dependency.ByName "simonmar/async:async"
          , Schema.Dependency.ByName "haskell/stm:stm"
          , Schema.Dependency.ByName "haskell/bytestring:bytestring"
          , Schema.Dependency.ByName "haskell/text:text"
          , pinned "iand675/hs-opentelemetry:hs-opentelemetry-api" "^>=1.0"
          , pinned "unordered-containers" "^>=0.2"
          , testOnly "iand675/hs-opentelemetry:hs-opentelemetry-exporter-in-memory"
          , testOnly "iand675/hs-opentelemetry:hs-opentelemetry-sdk"
          , testOnly "haskell/process:process"
          ]
        }
      ]
    , dependencies =
      [ "shinzui/shibuya:shibuya-core"
      , "informatikr/hedis:hedis"
      , "effectful/effectful:effectful-core"
      , "composewell/streamly:streamly-core"
      , "simonmar/async:async"
      , "haskell/stm:stm"
      , "haskell/bytestring:bytestring"
      , "haskell/text:text"
      , "iand675/hs-opentelemetry:hs-opentelemetry-api"
      , "unordered-containers"
      , "iand675/hs-opentelemetry:hs-opentelemetry-exporter-in-memory"
      , "iand675/hs-opentelemetry:hs-opentelemetry-sdk"
      , "haskell/process:process"
      ]
    , dependencyRefs =
      [ pkgRef "shinzui" "shibuya" "shibuya-core"
      , pkgRef "informatikr" "hedis" "hedis"
      , pkgRef "effectful" "effectful" "effectful-core"
      , pkgRef "composewell" "streamly" "streamly-core"
      , pkgRef "simonmar" "async" "async"
      , pkgRef "haskell" "stm" "stm"
      , pkgRef "haskell" "bytestring" "bytestring"
      , pkgRef "haskell" "text" "text"
      , pkgRef "iand675" "hs-opentelemetry" "hs-opentelemetry-api"
      , pkgRef "iand675" "hs-opentelemetry" "hs-opentelemetry-exporter-in-memory"
      , pkgRef "iand675" "hs-opentelemetry" "hs-opentelemetry-sdk"
      , pkgRef "haskell" "process" "process"
      ]
    , agents =
      [ Schema.AgentHint::{
        , role = "adapter-dev"
        , description = Some
            "Redis Streams adapter development: XREAD readers, consumer groups, dead-lettering, runner fixture"
        , includePaths = [ "src/**", "test/**", "docs/**" ]
        , excludePaths = [ "dist-newstyle/**" ]
        , relatedPackages = [ "shibuya-redis-adapter" ]
        }
      ]
    , okfBundles =
      [ Schema.OkfBundle::{
        , name = "friction-log"
        , path = "docs/friction-log"
        , profile = Some "docs/friction-log/profile.dhall"
        , okfVersion = "0.2"
        , description = Some
            "Friction met while implementing plans, before triage"
        }
      , Schema.OkfBundle::{
        , name = "user-documentation"
        , path = "docs/user"
        , profile = Some "docs/user/profile.dhall"
        , profileBinding = okfBinding "documentation.userDocumentation"
        , okfVersion = "0.2"
        , description = Some "Integration guides and delivery semantics for Redis Streams processors"
        }
      , Schema.OkfBundle::{
        , name = "capabilities"
        , path = "docs/capabilities"
        , profile = Some "docs/capabilities/profile.dhall"
        , profileBinding = okfBinding "coordination.capabilities"
        , okfVersion = "0.2"
        , description = Some "Consumer-facing adapter capabilities, limits and repository evidence"
        }
      ]
    }
