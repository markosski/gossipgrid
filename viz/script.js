d3.json("sample_data.json").then(data => {
  const addresses = Array.from(new Set(data.flatMap(d => [d.address_from, d.address_to])));
  const width = 800, height = 600, radius = 200, centerX = width / 2, centerY = height / 2;
  const angleStep = (2 * Math.PI) / addresses.length;

  const nodes = addresses.map((id, i) => ({
    id,
    x: centerX + radius * Math.cos(i * angleStep),
    y: centerY + radius * Math.sin(i * angleStep),
  }));

  const svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);

  // Add arrowhead marker definition
  svg.append("defs").append("marker")
    .attr("id", "arrowhead")
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 30)
    .attr("refY", 0)
    .attr("markerWidth", 6)
    .attr("markerHeight", 6)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M0,-5L10,0L0,5")
    .attr("fill", "green");


  // Draw nodes
  svg.selectAll("circle")
    .data(nodes)
    .enter()
    .append("circle")
    .attr("cx", d => d.x)
    .attr("cy", d => d.y)
    .attr("r", 20)
    .attr("fill", "steelblue");

  svg.selectAll("text.node-label")
    .data(nodes)
    .enter()
    .append("text")
    .attr("class", "node-label")
    .attr("x", d => d.x)
    .attr("y", d => d.y - 30)
    .attr("text-anchor", "middle")
    .text(d => d.id);

  const slider = document.getElementById("stepSlider");
  const label = document.getElementById("infoLabel");
  slider.max = data.length - 1;

  function drawStep(index) {
    // Clear old messages
    svg.selectAll(".msg, .msg-text").remove();

    const msg = data[index];
    const sender = nodes.find(n => n.id === msg.address_from);
    const receiver = nodes.find(n => n.id === msg.address_to);
    if (!sender || !receiver) return;

    const messageLabel = msg.message_type +
      (msg.data !== undefined ? " \n\r" + JSON.stringify(msg.data) : "");

    label.textContent = `${msg.timestamp} | ${msg.address_from} → ${msg.address_to} | ${messageLabel}`;

    if (msg.address_from === msg.address_to) {
      const arcPath = d3.path();
      arcPath.moveTo(sender.x, sender.y);
      arcPath.arc(sender.x, sender.y - 30, 20, Math.PI / 2, 2.5 * Math.PI);

      svg.append("path")
        .attr("class", "msg")
        .attr("d", arcPath.toString())
        .attr("stroke", "green")
        .attr("stroke-width", 2)
        .attr("fill", "none");

      svg.append("text")
        .attr("class", "msg-text")
        .attr("x", sender.x)
        .attr("y", sender.y - 50)
        .attr("text-anchor", "middle")
        .attr("fill", "black")
        .text(messageLabel);
    } else {
      svg.append("line")
        .attr("class", "msg")
        .attr("x1", sender.x)
        .attr("y1", sender.y)
        .attr("x2", receiver.x)
        .attr("y2", receiver.y)
        .attr("stroke", "green")
        .attr("stroke-width", 2)
        .attr("marker-end", "url(#arrowhead)");

      svg.append("text")
        .attr("class", "msg-text")
        .attr("x", (sender.x + receiver.x) / 2)
        .attr("y", (sender.y + receiver.y) / 2 + 20)
        .attr("text-anchor", "middle")
        .attr("fill", "black")
        .text(messageLabel);
    }
  }

  // Initial render
  drawStep(0);

  slider.addEventListener("input", e => {
    drawStep(+e.target.value);
  });
});
