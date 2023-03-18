# Mermaid Chart

[Mermaid](https://mermaidjs.github.io/) is library for generating svg charts and diagrams from text.

## Example

{{< columns >}}
```tpl
{{</* mermaid [class="text-center"]*/>}}
sequenceDiagram
    Alice->>Bob: Hello Bob, how are you?
    alt is sick
        Bob->>Alice: Not so good :(
    else is well
        Bob->>Alice: Feeling fresh like a daisy
    end
    opt Extra response
        Bob->>Alice: Thanks for asking
    end
{{</* /mermaid */>}}
```

<--->

{{< mermaid >}}
sequenceDiagram
    Alice->>Bob: Hello Bob, how are you?
    alt is sick
        Bob->>Alice: Not so good :(
    else is well
        Bob->>Alice: Feeling fresh like a daisy
    end
    opt Extra response
        Bob->>Alice: Thanks for asking
    end
{{< /mermaid >}}

{{< /columns >}}
