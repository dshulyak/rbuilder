
# goals

## reusable/protected components

redundant code that relied on reth implementation details. 
that potentially lead to problematic reth updates. 
and would be especially painful if same components were reused for other builders

## customizable builders

impossible to customize or extend builders. if 20 different L2's will need slightly different builders it would lead to enourmous redundancy. 
and it will also have secondary effects, such as if you want to introduce new pipeline it will be harder to adopt by those 20 builders.

## future proof integrations

insufficient abstractions for versioning and introducing other breaking changes

# solution

## components

in order to protect codebase (and integrations) from the changes in reth i introduced
components concept, and one example of that is TxExecutor that hides code that interacts with EVM and receipts generation.
Go over that component.

Show TxExecutor.

The end goal here is to have components that do not expose types or traits from reth/revm.
Important for project velocity but especially if rbuilder will be used as a library.

## actions

We need to make builder itself composable. So that customizations only re-define essential part of the builder,
without copying the rest.

for this purpose i splitted existing builders into resuable actions. [go over them]
each such action implements specific interface, that is assumed to somewhat resilient to a change.

motivating example is world chain builder that changes only one action, and this is the only thing that
should be maintained in their codebase.

beside that certain parts are not that likely to change, however there is still an advantage for them to be customizable.
there are often changes that are not backward compatible in terms of behavior so you want to release a new version
and require minimal modifications on the user side to opt-in (versioning, bug fixes). 

## strategies

actions are composed into strategies. there are obviously two such strategies:
- vanilla
- flashblocks

the idea here is that you will add more strategies over time, that may not preserve previously strategy behavior.
in such case you will release a new strategy, reusing actions.

the motivating example here, is what it would take world chain builder to adopt flashblocks?
ideally we implemented action with their specific tx logic. and if we introduce new strategy, we can action
into new strategy.

# future work

## generic traits

i didn't finish this refactoring and it would take a lot of time to get rid of relience 
on concrete types.

## get rid for reth/revm in api's

majority of the codebase was inherited from reth.
to provide a "clean" solution we have to redefine our own types and traits.
in some cases it may require bringing in larger chunks of code from reth itself.

## alternative composability design

there is more than one way to break actions and merge them into strategies.
i wrote one that seemed the most straightforward to me.

i considered alternative where you compose structs that implement certain traits.
and we can see what is the right way to go when doing 2 things above.
