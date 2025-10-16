import { exec } from "child_process";

console.log("Simulating a mid-run crash...");

exec("docker compose kill api", (err, stdout, stderr) => {
  if (err) {
    console.error(`Error killing container: ${stderr}`);
    return;
  }
  console.log('Container "api" has been killed to simulate a crash.');
});
