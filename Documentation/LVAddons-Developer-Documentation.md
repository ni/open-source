# LVAddons:  System-level Installation for LabVIEW Drivers, Toolkits, and More

## Overview

LVAddons is a system-level installation location for LabVIEW drivers, toolkits, and other add-ons. 
LabVIEW (2022 Q3 and later) includes features that enable it to use add-ons installed in LVAddons.

## Benefits
When upgrading LabVIEW, you are not required to install new versions of drivers and other add-ons in LVAddons.
The newer version of LabVIEW will automatically use the add-ons that were used by the older version. 
You can upgrade your add-ons when you want to take advantage of new capabilities or bug fixes.

## Location of LVAddons
By default, the root of LVAddons is located at **C:\\Program Files\\NI\\LVAddons**.
You can change the location using the LabVIEW configuration token *LVAddons.CustomLocation*.

You can optionally have additional locations using the following configuration tokens:
1. *LVAddons.AdditionalLocations* - a list of paths separated by semicolons
2. Per-target *LibraryPaths* - a list of paths separated by semicolons. For example:
   - LocalHost.LibraryPaths
   - NI.RT.LINUX.PXI.LibraryPaths
   - NI.RT.CDAQ.Linux.LibraryPaths

## Using LVAddons
In general, you install and use add-ons in LVAddons the same way that do for add-ons in the LabVIEW folder.
However, the first time you use the add-on, it will take longer to load.
LVAddons requires that VIs separate compiled code from source files, and when LabVIEW loads them for the first time, it compiles them and stores the compiled code in a cache specific to that LabVIEW version.

## Creating LVAddons

### File Structure
You will need a uniquely-named add-on folder inside LVAddons.
NI recommends:
1. Using a short folder name (e.g., "nidaqmx") to avoid potential issues with long paths.
2. Using a company prefix to avoid name collisions.
3. Appending 32 or 64 for add-ons that are specific to 32-bit or 64-bit.

Within your add-on folder, you need a version folder for your add-on (e.g., "1"). 
LabVIEW will use the highest version of an add-on that supports that version of LabVIEW.
Note: there's currently a bug where the version folders are ordered alphabetically instead of numerically when determining the highest version.

Within your add-on version folder, you can have the following folders, which correspond to folders inside the LabVIEW folder:
1. examples
2. help
3. instr.lib
4. manuals
5. menus
6. project
7. ProjectTemplates
8. resource
   - Note: resource\dialog\QuickDrop\plugins is supported in LabVIEW 2025 Q1 and later.
9. Targets\NI\RT
10. Targets\win (supported in LabVIEW 2023 Q3 and later)
11. Targets\win32 (supported in LabVIEW 2023 Q3 and later)
12. Targets\win64 (supported in LabVIEW 2023 Q3 and later)
13. Targets\linux (supported in LabVIEW 2023 Q3 and later)
14. templates
15. vi.lib
16. vi.lib\_probes

### JSON Descriptor File
Within your add-on version folder, you must have a file with the name **lvaddoninfo.json**.
LabVIEW uses this file to identify an add-on and to know what minimum version of LabVIEW it supports.

The file must define three attributes:
1. **AddonName**. This must match your add-on folder name.
2. **ApiVersion**. Required but not currently used. NI recommends using a "v" followed by the name of your version folder.
3. **MinimumSupportedLVVersion** (e.g., "24.0"). Note: NI recommends that this value be "22.3" or later, even if the VIs are compatible with older versions of LabVIEW, since LabVIEW did not support LVAddons before LabVIEW 2022 Q3.

The file can have the following optional attributes:
1. **SupportedBitness**. If present, it must be set to 32 or 64. If not present, it means the add-on supports both. Note: for LabVIEW 2023 Q3 and later, you can use the Targets folder instead of this attribute.

For example, this is the descriptor file for the VI Analyzer:
```
{
    "AddonName": "viawin",
    "ApiVersion": "v1",
    "MinimumSupportedLVVersion": "24.0"
}
```

### Symbolic Paths
Before LVAddons, symbolic paths (such as \<vi.lib\>) could resolve to a specific folder for a given version of LabVIEW.
In newer versions of LabVIEW, symbolic paths are a list of locations instead.
For example, LabVIEW will virtually combine all the **vi.lib** folders under LVAddons with the **vi.lib** in the LabVIEW folder.

To see the list of resolved paths for a symbolic path, you can use the function **vi.lib\Utility\Symbolic Paths\Build and Resolve Symbolic Path.vi**.
When resolving symbolic paths, LabVIEW uses the following precedence:
1. LabVIEW folder
2. LibraryPaths (first wins)
3. Active LVAddons (first wins)

When selecting active LVAddons, LabVIEW uses the following precedence:
1. Highest version (folder name) that supports the current LabVIEW version and bitness
2. Default and additional LVAddons root folders (last wins)


### File Requirements
- LabVIEW files (VIs, libraries, and classes) in LVAddons must separate compiled code from source files.
- LabVIEW files must have a save version that is no later than the minimum supported version of the add-on. Note that LabVIEW 2024 Q3 and later allows you to easily save VIs, libraries, and classes in older save versions.

### Packed Library (PPL) Considerations
Packed Library builds must enable "Allow future versions of LabVIEW to load this packed library" in Properties>Advanced.
NI recommends putting the 32-bit and 64-bit builds of a packed library in their respective *Targets* folders.
Note that (as mentioned above) the *Targets* folders for desktop LabVIEW are only supported in LabVIEW 2023 Q3 and later.

## Editor Tips When Developing for LVAddons
You typically want to develop files in a location that's backed by a source control system (e.g., C:\\dev\\MyAddon), but LabVIEW loads add-ons from LVAddons. As noted above, you can use configuration tokens to define alternative or additional locations for LVAddons.

You can use different configuration files for different projects by specifying the configuration file on the command line: 
```
labview.exe -pref <path to config file>
```

NI recommends keeping your configuration file with your project in source control and using a script to launch LabVIEW to use it.
