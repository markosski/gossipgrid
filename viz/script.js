const data = [
    { timestamp: "2025-03-08T20:41:50Z", sender: "127.0.0.1:4115", receiver: "127.0.0.1:4109", message: "Peers: {4109, 4115}" },
    { timestamp: "2025-03-08T20:41:51Z", sender: "127.0.0.1:4109", receiver: "127.0.0.1:4116", message: "Peers: {4109, 4115}" },
    { timestamp: "2025-03-08T20:41:55Z", sender: "127.0.0.1:4115", receiver: "127.0.0.1:4109", message: "Peers: {4109, 4115}" },
    { timestamp: "2025-03-08T20:42:00Z", sender: "127.0.0.1:4115", receiver: "127.0.0.1:4109", message: "Peers: {4109, 4115, 4116}" },
    { timestamp: "2025-03-08T20:42:01Z", sender: "127.0.0.1:4109", receiver: "127.0.0.1:4115", message: "Peers: {4109, 4115, 4116}" }
  ];
  
  const width = 800, height = 600;
  const svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);
  
  const nodes = ["127.0.0.1:4109", "127.0.0.1:4115", "127.0.0.1:4116"].map((node, i) => ({
    id: node,
    x: width / 4 * (i + 1),
    y: height / 2
  }));
  
  const nodeElements = svg.selectAll("circle")
    .data(nodes)
    .enter()
    .append("circle")
    .attr("cx", d => d.x)
    .attr("cy", d => d.y)
    .attr("r", 20)
    .attr("fill", "blue");
  
  svg.selectAll("text")
    .data(nodes)
    .enter()
    .append("text")
    .attr("x", d => d.x)
    .attr("y", d => d.y - 30)
    .attr("text-anchor", "middle")
    .text(d => d.id);
  
  function animateMessages() {
    data.forEach((msg, i) => {
      setTimeout(() => {
        const sender = nodes.find(n => n.id === msg.sender);
        const receiver = nodes.find(n => n.id === msg.receiver);
        
        svg.append("line")
          .attr("x1", sender.x)
          .attr("y1", sender.y)
          .attr("x2", sender.x)
          .attr("y2", sender.y)
          .attr("stroke", "red")
          .attr("stroke-width", 2)
          .transition()
          .duration(1000)
          .attr("x2", receiver.x)
          .attr("y2", receiver.y)
          .remove();
  
        svg.append("text")
          .attr("x", receiver.x)
          .attr("y", receiver.y + 30)
          .attr("text-anchor", "middle")
          .attr("fill", "black")
          .text(msg.message)
          .transition()
          .duration(1500)
          .style("opacity", 0)
          .remove();
      }, i * 1500);
    });
  }
  
  animateMessages();
  