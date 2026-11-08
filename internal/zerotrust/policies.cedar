// who may do what to links. principals are User::"name" (from a
// header here, a jwt in real life) or Service::"name" (from a spiffe id on
// an mtls client cert, which can't be faked by setting a header).

// signed in users shorten links and follow them
permit (principal is User, action in [Action::"Create", Action::"Resolve"], resource)
when { principal != User::"anonymous" && !context.vanity };

// picking your own slug is a pro feature
permit (principal in Role::"pro", action == Action::"Create", resource);

// owners delete their own links
permit (principal, action == Action::"Delete", resource)
when { resource has owner && resource.owner == principal };

// admins see and remove everything
permit (principal in Role::"admin", action in [Action::"Delete", Action::"List"], resource);

// the redirector only ever resolves
permit (principal == Service::"redirector", action == Action::"Resolve", resource);

// the operator manages links on behalf of ShortLink objects
permit (principal == Service::"operator", action, resource)
when { action != Action::"List" };

// nobody, admins included, shortens to internal hosts
@id("no-internal-targets")
forbid (principal, action == Action::"Create", resource)
when { context.targetHost == "localhost" || context.targetHost like "*.internal" };
