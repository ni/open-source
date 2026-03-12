### Parameters
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |

### Dispatcher Functions

#### Invoke-AddTokenToLabVIEW
Adds an authentication token to a LabVIEW installation. MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). RelativePath: Normalized path to the project root relative to the working directory. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the project supports |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-AddTokenToLabVIEW -ArgsJson '{}'
```

#### Invoke-ApplyVIPC
Applies a VI Package Configuration to a LabVIEW project. MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports. VIP_LVVersion: LabVIEW version used to build the VIPC. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). RelativePath: Normalized path to the project root relative to the working directory. VIPCPath: Optional path to the VIPC file. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the project supports |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| VIPCPath | string | false |  | Optional path to the VIPC file |
| VIP_LVVersion | string | true |  | LabVIEW version used to build the VIPC |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-ApplyVIPC -ArgsJson '{}'
```

#### Invoke-Build
Builds the project and records version information. RelativePath: Normalized path to the project root relative to the working directory. Major: Major version component. Minor: Minor version component. Patch: Patch version component. Build: Build number component. Commit: Commit identifier used for the build metadata. LabVIEWMinorRevision: Minor revision of LabVIEW used for the build. CompanyName: Company name recorded in build metadata. AuthorName: Author name recorded in build metadata. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| AuthorName | string | true |  | Author name recorded in build metadata |
| Build | number | true |  | Build number component |
| Commit | string | true |  | Commit identifier used for the build metadata |
| CompanyName | string | true |  | Company name recorded in build metadata |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LabVIEWMinorRevision | string | true |  | Minor revision of LabVIEW used for the build |
| Major | number | true |  | Major version component |
| Minor | number | true |  | Minor version component |
| Patch | number | true |  | Patch version component |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-Build -ArgsJson '{}'
```

#### Invoke-BuildLvlibp
Builds a LabVIEW Packed Library using a project and build spec. MinimumSupportedLVVersion: Minimum LabVIEW version that the library supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). RelativePath: Normalized path to the project root relative to the working directory. LabVIEW_Project: Path to the LabVIEW project file. Build_Spec: Name of the build specification within the project. Major: Major version component. Minor: Minor version component. Patch: Patch version component. Build: Build number component. Commit: Commit identifier used for the build metadata. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build | number | true |  | Build number component |
| Build_Spec | string | true |  | Name of the build specification within the project |
| Commit | string | true |  | Commit identifier used for the build metadata |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LabVIEW_Project | string | true |  | Path to the LabVIEW project file |
| Major | number | true |  | Major version component |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the library supports |
| Minor | number | true |  | Minor version component |
| Patch | number | true |  | Patch version component |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-BuildLvlibp -ArgsJson '{}'
```

#### Invoke-BuildLvlibpDockerLinux
Builds LabVIEW Packed Project Library (.lvlibp) using Linux Docker container. MinimumSupportedLVVersion: LabVIEW version for the build (e.g., "2021", "2026"). SupportedBitness: Bitness of the LabVIEW environment ("32" or "64"). ProjectPath: Path to the LabVIEW project .lvproj file. TargetName: Target that contains the build specification (optional, defaults to "My Computer"). BuildSpecName: Name of the LabVIEW build specification (optional, builds all if empty). Major: Major version component (optional, skips version setting if < 0). Minor: Minor version component (optional, skips version setting if < 0). Patch: Patch version component (optional, skips version setting if < 0). Build: Build number component (optional, skips version setting if < 0). Commit: Commit hash or identifier (optional). DockerImage: Docker image name (default: "nationalinstruments/labview"). ImageTag: Docker image tag (defaults to "2026q1-linux"). DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build | number | false | -1 | Build number component (optional, skips version setting if < 0) |
| BuildSpecName | string | false |  | Name of the LabVIEW build specification (optional, builds all if empty) |
| Commit | string | false |  | Commit hash or identifier (optional) |
| DockerImage | string | false | nationalinstruments/labview | Docker image name (default: "nationalinstruments/labview") |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| ImageTag | string | false | 2026q1-linux | Docker image tag (defaults to "2026q1-linux") |
| Major | number | false | -1 | Major version component (optional, skips version setting if < 0) |
| MinimumSupportedLVVersion | string | true |  | LabVIEW version for the build (e |
| Minor | number | false | -1 | Minor version component (optional, skips version setting if < 0) |
| Patch | number | false | -1 | Patch version component (optional, skips version setting if < 0) |
| ProjectPath | string | true |  | Path to the LabVIEW project |
| SupportedBitness | string | true |  | Bitness of the LabVIEW environment ("32" or "64") |
| TargetName | string | false |  | Target that contains the build specification (optional, defaults to "My Computer") |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-BuildLvlibpDockerLinux -ArgsJson '{}'
```

#### Invoke-BuildLvlibpDockerWindows
Builds LabVIEW Packed Project Library (.lvlibp) using Windows Docker container. MinimumSupportedLVVersion: LabVIEW version for the build (e.g., "2021", "2026"). SupportedBitness: Bitness of the LabVIEW environment ("32" or "64"). ProjectPath: Path to the LabVIEW project .lvproj file. TargetName: Target that contains the build specification (optional, defaults to "My Computer"). BuildSpecName: Name of the LabVIEW build specification (optional, builds all if empty). Major: Major version component (optional, skips version setting if < 0). Minor: Minor version component (optional, skips version setting if < 0). Patch: Patch version component (optional, skips version setting if < 0). Build: Build number component (optional, skips version setting if < 0). Commit: Commit hash or identifier (optional). DockerImage: Docker image name (default: "nationalinstruments/labview"). ImageTag: Docker image tag (defaults to "2026q1-windows"). DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build | number | false | -1 | Build number component (optional, skips version setting if < 0) |
| BuildSpecName | string | false |  | Name of the LabVIEW build specification (optional, builds all if empty) |
| Commit | string | false |  | Commit hash or identifier (optional) |
| DockerImage | string | false | nationalinstruments/labview | Docker image name (default: "nationalinstruments/labview") |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| ImageTag | string | false | 2026q1-windows | Docker image tag (defaults to "2026q1-windows") |
| Major | number | false | -1 | Major version component (optional, skips version setting if < 0) |
| MinimumSupportedLVVersion | string | true |  | LabVIEW version for the build (e |
| Minor | number | false | -1 | Minor version component (optional, skips version setting if < 0) |
| Patch | number | false | -1 | Patch version component (optional, skips version setting if < 0) |
| ProjectPath | string | true |  | Path to the LabVIEW project |
| SupportedBitness | string | true |  | Bitness of the LabVIEW environment ("32" or "64") |
| TargetName | string | false |  | Target that contains the build specification (optional, defaults to "My Computer") |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-BuildLvlibpDockerWindows -ArgsJson '{}'
```

#### Invoke-BuildLvlibpWin32
Builds LabVIEW Packed Project Library (.lvlibp) using Windows GitHub-hosted runner. MinimumSupportedLVVersion: LabVIEW version for the build (e.g., "2021", "2026"). SupportedBitness: Bitness of the LabVIEW environment ("32" or "64"). ProjectPath: Path to the LabVIEW project .lvproj file. TargetName: Target that contains the build specification. BuildSpecName: Name of the LabVIEW build specification (optional, builds all if empty). Major: Major version component. Minor: Minor version component. Patch: Patch version component. Build: Build number component. Commit: Commit hash or identifier. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build | number | true |  | Build number component |
| BuildSpecName | string | false |  | Name of the LabVIEW build specification (optional, builds all if empty) |
| Commit | string | true |  | Commit hash or identifier |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| Major | number | true |  | Major version component |
| MinimumSupportedLVVersion | string | true |  | LabVIEW version for the build (e |
| Minor | number | true |  | Minor version component |
| Patch | number | true |  | Patch version component |
| ProjectPath | string | true |  | Path to the LabVIEW project |
| SupportedBitness | string | true |  | Bitness of the LabVIEW environment ("32" or "64") |
| TargetName | string | true |  | Target that contains the build specification |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-BuildLvlibpWin32 -ArgsJson '{}'
```

#### Invoke-BuildViPackage
Builds a VI Package using the provided VIPB file and version metadata. MinimumSupportedLVVersion: Minimum LabVIEW version that the package supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). LabVIEWMinorRevision: Minor revision of LabVIEW used to build the package. RelativePath: Normalized path to the project root relative to the working directory. VIPBPath: Path to the VIPB build specification file. Major: Major version component. Minor: Minor version component. Patch: Patch version component. Build: Build number component. Commit: Commit identifier used for the build metadata. DisplayInformationJSON: JSON string containing display information for the package. ReleaseNotesFile: Optional path to a release notes file. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build | number | true |  | Build number component |
| Commit | string | true |  | Commit identifier used for the build metadata |
| DisplayInformationJSON | string | true |  | JSON string containing display information for the package |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LabVIEWMinorRevision | string | true |  | Minor revision of LabVIEW used to build the package |
| Major | number | true |  | Major version component |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the package supports |
| Minor | number | true |  | Minor version component |
| Patch | number | true |  | Patch version component |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| ReleaseNotesFile | string | false |  | Optional path to a release notes file |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| VIPBPath | string | true |  | Path to the VIPB build specification file |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-BuildViPackage -ArgsJson '{}'
```

#### Invoke-CloseLabVIEW
Closes any running instance of LabVIEW. MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the project supports |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-CloseLabVIEW -ArgsJson '{}'
```

#### Invoke-GenerateReleaseNotes
Generates a release notes file from the project's metadata. OutputPath: Path where the release notes should be written. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| OutputPath | string | false | Tooling/deployment/release_notes.md | Path where the release notes should be written |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-GenerateReleaseNotes -ArgsJson '{}'
```

#### Invoke-MissingInProject
Lists files referenced in a LabVIEW project that are missing on disk. LVVersion: LabVIEW version of the project. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). ProjectFile: Path to the .lvproj file to analyze. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LVVersion | string | true |  | LabVIEW version of the project |
| ProjectFile | string | true |  | Path to the |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-MissingInProject -ArgsJson '{}'
```

#### Invoke-ModifyVIPBDisplayInfo
Updates display information fields in a VIPB build specification. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). RelativePath: Normalized path to the project root relative to the working directory. VIPBPath: Path to the VIPB build specification file. MinimumSupportedLVVersion: Minimum LabVIEW version that the package supports. LabVIEWMinorRevision: Minor revision of LabVIEW used for the build. Major: Major version component. Minor: Minor version component. Patch: Patch version component. Build: Build number component. Commit: Commit identifier used for the build metadata. DisplayInformationJSON: JSON string containing display information for the package. ReleaseNotesFile: Optional path to a release notes file. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build | number | true |  | Build number component |
| Commit | string | true |  | Commit identifier used for the build metadata |
| DisplayInformationJSON | string | true |  | JSON string containing display information for the package |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LabVIEWMinorRevision | string | true |  | Minor revision of LabVIEW used for the build |
| Major | number | true |  | Major version component |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the package supports |
| Minor | number | true |  | Minor version component |
| Patch | number | true |  | Patch version component |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| ReleaseNotesFile | string | false |  | Optional path to a release notes file |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| VIPBPath | string | true |  | Path to the VIPB build specification file |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-ModifyVIPBDisplayInfo -ArgsJson '{}'
```

#### Invoke-PrepareLabVIEWSource
Prepares a LabVIEW project for source distribution. MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). RelativePath: Normalized path to the project root relative to the working directory. LabVIEW_Project: Path to the LabVIEW project file. Build_Spec: Name of the build specification within the project. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build_Spec | string | true |  | Name of the build specification within the project |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LabVIEW_Project | string | true |  | Path to the LabVIEW project file |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the project supports |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-PrepareLabVIEWSource -ArgsJson '{}'
```

#### Invoke-RenameFile
Renames a file on disk. CurrentFilename: Existing path to the file. NewFilename: New path for the file. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| CurrentFilename | string | true |  | Existing path to the file |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| NewFilename | string | true |  | New path for the file |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-RenameFile -ArgsJson '{}'
```

#### Invoke-RestoreSetupLVSource
Restores the Setup LabVIEW source build specification. MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). RelativePath: Normalized path to the project root relative to the working directory. LabVIEW_Project: Path to the LabVIEW project file. Build_Spec: Name of the build specification within the project. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Build_Spec | string | true |  | Name of the build specification within the project |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| LabVIEW_Project | string | true |  | Path to the LabVIEW project file |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the project supports |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-RestoreSetupLVSource -ArgsJson '{}'
```

#### Invoke-RevertDevelopmentMode
Returns a repository to its previous development mode state. RelativePath: Normalized path to the project root relative to the working directory. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-RevertDevelopmentMode -ArgsJson '{}'
```

#### Invoke-RunPesterTests
Runs Pester tests located in the specified working directory. WorkingDirectory: Path containing the Pester tests to execute. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| WorkingDirectory | string | true |  | Path containing the Pester tests to execute |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-RunPesterTests -ArgsJson '{}'
```

#### Invoke-RunUnitTests
Runs LabVIEW unit tests. MinimumSupportedLVVersion: Minimum LabVIEW version that the project supports. SupportedBitness: Target LabVIEW bitness (32- or 64-bit). DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| MinimumSupportedLVVersion | string | true |  | Minimum LabVIEW version that the project supports |
| SupportedBitness | string | true |  | Target LabVIEW bitness (32- or 64-bit) |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-RunUnitTests -ArgsJson '{}'
```

#### Invoke-SetDevelopmentMode
Configures the repository for development mode. RelativePath: Normalized path to the project root relative to the working directory. DryRun: If set, prints the command instead of executing it. gcliPath: Optional path prepended to PATH for locating the g CLI.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| DryRun | boolean | false |  | If set, prints the command instead of executing it |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |
| gcliPath | string | false |  | Optional path prepended to PATH for locating the g CLI |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Invoke-SetDevelopmentMode -ArgsJson '{}'
```

#### Normalize-RelativePath
Normalizes a RelativePath value against an optional base directory. RelativePath: Path to normalize. BaseDirectory: Directory used to resolve the relative path. Defaults to the current location.
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| BaseDirectory | string | false |  | Directory used to resolve the relative path |
| RelativePath | string | true |  | Path relative to WorkingDirectory that locates the project or repository root. |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Normalize-RelativePath -ArgsJson '{}'
```

#### Set-LogLevel
Sets the verbosity for informational and verbose messages. Level: Desired log level (ERROR, WARN, INFO, DEBUG).
| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| Level | string | false |  | Desired log level (ERROR, WARN, INFO, DEBUG) |

```powershell
pwsh ./actions/Invoke-OSAction.ps1 -ActionName Set-LogLevel -ArgsJson '{}'
```

### Wrapper Actions

#### add-token-to-labview
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| relative_path | string | true |  | Relative path containing the token target. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### apply-vipc
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| vip_lv_version | string | true |  | LabVIEW version associated with the VI Package. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| relative_path | string | true |  | Relative path containing the LabVIEW project. |
| vipc_path | string | false |  | Path to the VIPC file. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### build
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| relative_path | string | true |  | Relative path containing the LabVIEW project. |
| major | string | true |  | Major version component. |
| minor | string | true |  | Minor version component. |
| patch | string | true |  | Patch version component. |
| build | string | true |  | Build number. |
| commit | string | true |  | Commit identifier. |
| labview_minor_revision | string | true |  | LabVIEW minor revision. |
| company_name | string | true |  | Company name for the build. |
| author_name | string | true |  | Author name for the build. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### build-lvlibp
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| relative_path | string | true |  | Relative path containing the LabVIEW project. |
| labview_project | string | true |  | Path to the LabVIEW project file. |
| build_spec | string | true |  | Name of the build specification. |
| major | string | true |  | Major version component. |
| minor | string | true |  | Minor version component. |
| patch | string | true |  | Patch version component. |
| build | string | true |  | Build number. |
| commit | string | true |  | Commit identifier. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### build-lvlibp-docker-linux
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | LabVIEW version year for the build (e.g., "2021", "2026"). |
| supported_bitness | string | true |  | Bitness of the LabVIEW environment ("32" or "64"). |
| project_path | string | true |  | Path to the LabVIEW project .lvproj file. |
| target_name | string | false |  | Target that contains the build specification. Defaults to "My Computer" in helper VI. |
| build_spec_name | string | false |  | Name of the build specification. If empty, builds all specifications in the target. |
| major | string | false |  | Major version component for the PPL. Omit to skip version setting. |
| minor | string | false |  | Minor version component for the PPL. Omit to skip version setting. |
| patch | string | false |  | Patch version component for the PPL. Omit to skip version setting. |
| build | string | false |  | Build number component for the PPL. Omit to skip version setting. |
| commit | string | false |  | Commit hash or identifier recorded in the build. |
| docker_image | string | false | nationalinstruments/labview | Docker image name. |
| image_tag | string | false | 2026q1-linux | Docker image tag. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### build-lvlibp-docker-windows
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | LabVIEW version year for the build (e.g., "2021", "2026"). |
| supported_bitness | string | true |  | Bitness of the LabVIEW environment ("32" or "64"). |
| project_path | string | true |  | Path to the LabVIEW project .lvproj file. |
| target_name | string | false |  | Target that contains the build specification. Defaults to "My Computer" in helper VI. |
| build_spec_name | string | false |  | Name of the build specification. If empty, builds all specifications in the target. |
| major | string | false |  | Major version component for the PPL. Omit to skip version setting. |
| minor | string | false |  | Minor version component for the PPL. Omit to skip version setting. |
| patch | string | false |  | Patch version component for the PPL. Omit to skip version setting. |
| build | string | false |  | Build number component for the PPL. Omit to skip version setting. |
| commit | string | false |  | Commit hash or identifier recorded in the build. |
| docker_image | string | false | nationalinstruments/labview | Docker image name. |
| image_tag | string | false | 2026q1-windows | Docker image tag. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### build-lvlibp-win32
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | LabVIEW version year for the build (e.g., "2021", "2026"). |
| supported_bitness | string | true |  | Bitness of the LabVIEW environment ("32"). |
| project_path | string | true |  | Path to the LabVIEW project .lvproj file. |
| target_name | string | true |  | Target that contains the build specification. |
| build_spec_name | string | false |  | Name of the build specification. If empty, builds all specifications in the target. |
| major | string | true |  | Major version component for the PPL. |
| minor | string | true |  | Minor version component for the PPL. |
| patch | string | true |  | Patch version component for the PPL. |
| build | string | true |  | Build number component for the PPL. |
| commit | string | true |  | Commit hash or identifier recorded in the build. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### build-vi-package
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| labview_minor_revision | string | true |  | LabVIEW minor revision. |
| relative_path | string | true |  | Relative path containing the project. |
| vipb_path | string | true |  | Path to the VIPB file. |
| major | string | true |  | Major version component. |
| minor | string | true |  | Minor version component. |
| patch | string | true |  | Patch version component. |
| build | string | true |  | Build number. |
| commit | string | true |  | Commit identifier. |
| display_information_json | string | true |  | JSON string of display information. |
| release_notes_file | string | false |  | Optional path to release notes file. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### close-labview
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### generate-release-notes
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| output_path | string | false | Tooling/deployment/release_notes.md | Path to output markdown file. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### missing-in-project
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| lv_version | string | true |  | LabVIEW version to use. |
| supported_bitness | string | true |  | Target LabVIEW bitness (32 or 64). |
| project_file | string | true |  | Path to the LabVIEW project (.lvproj). |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### modify-vipb-display-info
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| relative_path | string | true |  | Relative path containing the project. |
| vipb_path | string | true |  | Path to the VIPB file. |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| labview_minor_revision | string | true |  | LabVIEW minor revision. |
| major | string | true |  | Major version component. |
| minor | string | true |  | Minor version component. |
| patch | string | true |  | Patch version component. |
| build | string | true |  | Build number. |
| commit | string | true |  | Commit identifier. |
| display_information_json | string | true |  | JSON string of display information. |
| release_notes_file | string | false |  | Optional path to release notes file. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### prepare-labview-source
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| relative_path | string | true |  | Relative path containing the project. |
| labview_project | string | true |  | Path to the LabVIEW project file. |
| build_spec | string | true |  | Name of the build specification. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### rename-file
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| current_filename | string | true |  | Existing file name. |
| new_filename | string | true |  | New file name. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### restore-setup-lv-source
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | Minimum LabVIEW version supported. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| relative_path | string | true |  | Relative path containing the project. |
| labview_project | string | true |  | Path to the LabVIEW project file. |
| build_spec | string | true |  | Name of the build specification. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### revert-development-mode
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| relative_path | string | true |  | Relative path containing the repository. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### run-pester-tests
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| working_directory | string | true |  | Directory containing the repository under test. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### run-unit-tests
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| minimum_supported_lv_version | string | true |  | LabVIEW version for the test run. |
| supported_bitness | string | true |  | "32" or "64" bitness of LabVIEW. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### set-development-mode
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| relative_path | string | true |  | Relative path containing the repository. |
| gcli_path | string | false |  | Optional path to the g-cli executable. |
| working_directory | string | false |  | Working directory where the action will run. |
| log_level | string | false | INFO | Verbosity level (ERROR|WARN|INFO|DEBUG). |
| dry_run | string | false | false | If true, simulate the action without side effects. |

#### setup-mkdocs
| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |