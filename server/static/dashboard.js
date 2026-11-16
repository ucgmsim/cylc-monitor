// Click a dashboard table's column header to sort by it; click again to reverse.
// datasette-dashboards draws its tables after the page loads, so listen on the document.
// Numbers sort as numbers, everything else as text (ISO timestamps sort correctly as text);
// empty cells go last either way.
document.addEventListener("click", (event) => {
  const th = event.target.closest(".dashboard-card-chart th");
  if (!th) return;
  const table = th.closest("table");
  const tbody = table.tBodies[0];
  if (!tbody) return;
  const column = th.cellIndex;
  const ascending = th.getAttribute("aria-sort") !== "ascending";
  for (const other of th.parentElement.children) other.removeAttribute("aria-sort");
  th.setAttribute("aria-sort", ascending ? "ascending" : "descending");

  const text = (row) => (row.cells[column]?.textContent ?? "").trim();
  const numeric = (s) => s !== "" && s !== "null" && Number.isFinite(Number(s));
  const blank = (s) => s === "" || s === "null";
  const collator = new Intl.Collator(undefined, { numeric: true });
  const rows = Array.from(tbody.rows).map((row, index) => ({ row, index, value: text(row) }));
  rows.sort((a, b) => {
    if (blank(a.value) !== blank(b.value)) return blank(a.value) ? 1 : -1;
    let order;
    if (numeric(a.value) && numeric(b.value)) order = Number(a.value) - Number(b.value);
    else order = collator.compare(a.value, b.value);
    return (ascending ? order : -order) || a.index - b.index;
  });
  tbody.append(...rows.map((r) => r.row));
});
