<#-- To render the third-party file.
Available context :

- dependencyMap a collection of Map.Entry with
key are dependencies (as a MavenProject) (from the maven project)
values are licenses of each dependency (array of string)

- licenseMap a collection of Map.Entry with
key are licenses of each dependency (array of string)
values are all dependencies using this license
-->
Licenses of third-party dependencies
------------------------------------

<#list dependencyMap as e>
    <#assign project = e.getKey()/>
    <#assign licenses = e.getValue()/>
${project.name}, version ${project.version} (${project.groupId}:${project.artifactId})
    Description: ${project.description!""}
    Project URL: ${project.url!""}
    License:
    <#list project.licenses as license>
        ${license.name} (${license.url})
    </#list>
    <#--
    <#list licenses as license>
    ${license}
    </#list>
    -->

</#list>

Copies of the licenses are provided in the 'licenses' directory.

