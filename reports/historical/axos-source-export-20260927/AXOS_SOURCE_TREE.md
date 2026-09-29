# AXOS Source Tree

Generated 20260927T195037Z UTC from live filesystem at ~/workspace/axos/

```
axos/
├── audit/
│   ├── 01_write_path.py
│   ├── 02_transitions.py
│   ├── 03_leases.py
│   ├── 04_i17_i18.py
│   ├── 05_ledger.py
│   ├── 06_crash_backup_migration.py
│   ├── 07_race.py
│   ├── 08_retest.py
│   ├── 09_authority_audit.py
│   ├── _killpoints.py
│   ├── _migkill.py
│   └── stray_process_audit.py
├── exec/
│   ├── __init__.py
│   ├── boot.py
│   ├── finalizer.py
│   ├── identity.py
│   ├── policy.py
│   ├── reconciler.py
│   ├── recovery.py
│   ├── resilience.py
│   ├── scheduler.py
│   ├── supervisor.py
│   ├── synthetic.py
│   ├── watchdog.py
│   └── worker.py
├── store/
│   ├── __init__.py
│   ├── db.py
│   ├── gate.py
│   ├── migrations.py
│   ├── store.db
│   └── transitions.py
├── tests/
│   ├── _crasher.py
│   ├── _supdrv.py
│   ├── r5_helpers.py
│   ├── test_artifacts_r5.py
│   ├── test_boot_r6.py
│   ├── test_exec.py
│   ├── test_expiry_r4.py
│   ├── test_fence_enforce.py
│   ├── test_final_hardening_r14.py
│   ├── test_finalization_r13.py
│   ├── test_heartbeat_r3.py
│   ├── test_reclaim.py
│   ├── test_reconciliation_r11.py
│   ├── test_recovery_r8.py
│   ├── test_recovery_r9.py
│   ├── test_remediation.py
│   ├── test_resilience_r12.py
│   ├── test_scheduler_r10.py
│   ├── test_store.py
│   └── test_watchdog_r7.py
├── AUDIT-REPORT.md
├── PHASE-1C-FINAL-STATUS.md
├── PHASE1A-REPORT.md
├── R14-CONTRACT-AMENDMENT-PROPOSAL.md
├── R14-EVIDENCE.md
├── R14-EXTERNAL-VALIDATION.json
├── R14-EXTERNAL-VALIDATION.md
├── R14-PLAN.md
├── README.md
├── release_manifest.json
└── store.db
```

## Per-file details

| path | type | size_bytes | sha256 |
|---|---|---|---|
| axos/AUDIT-REPORT.md | file | 14934 | ec3b1256b7be117b20830a5b0bf58734b4f1f25b9568620c9243e5ff06fc7866 |
| axos/PHASE-1C-FINAL-STATUS.md | file | 11567 | d70b718c2d64cdf8d61b1cd100750e0b40b3b9602ea78c48c4ec59e974f0bd59 |
| axos/PHASE1A-REPORT.md | file | 5163 | b188cfd4a200f2618a6e703f0d2529e54a088fa674d3a6e1c90ac60fe6e8f307 |
| axos/R14-CONTRACT-AMENDMENT-PROPOSAL.md | file | 5359 | 17a73e567609f77fe68ef67c420895e1bfcfcfd60a401bdaba1ff3d166e63ac7 |
| axos/R14-EVIDENCE.md | file | 11866 | 6448fabd99d628f292228cd7e09cbef1e4f2f3df2e08bafe323ac1e988a0b68a |
| axos/R14-EXTERNAL-VALIDATION.json | file | 1096 | 88a0acbde6cbb7dd1d9771aceab293f766977954ecef540b38d13cd325806c97 |
| axos/R14-EXTERNAL-VALIDATION.md | file | 11442 | 8b354bd17271df4ef0a39053695bd7752cc6764e5cb7266f5aaaceb21fa0d5f3 |
| axos/R14-PLAN.md | file | 7691 | 3d20ca6cbcf17540b1a017c503152de230fa8d84b5b036a50f8a09a5fb691f15 |
| axos/README.md | file | 3770 | 7cba30f08853358af6bec870ab923e8a446b41f71a5eda46ad731e4a18923b3e |
| axos/audit/01_write_path.py | file | 7505 | c6fbaaea94f3fe49c97bf4782f8daff7e0d053ab807e4c2b7b50a06525b0174d |
| axos/audit/02_transitions.py | file | 11025 | 6b8e6e4306df1e235b70be5bd4acd671f967d875d881628d19228bbca1ccb6dc |
| axos/audit/03_leases.py | file | 8766 | 8da5365e9a0d374b2d11a265043b2e2993c3c41a17bf6767e884fc5c655570ae |
| axos/audit/04_i17_i18.py | file | 8922 | 461cdf2164854a39860cf35597d160df1758cfe04667ba6569b52f572d090276 |
| axos/audit/05_ledger.py | file | 5554 | da2ad1622881bbc5eca8790b0818e17a2acb76361fd729f4bab1e4a05713d9d1 |
| axos/audit/06_crash_backup_migration.py | file | 8572 | 4f2dae9367873f9da825474363bca7021a0d02c88a0d1af435abe03423913b17 |
| axos/audit/07_race.py | file | 1830 | 9c3e408346a6d28dcb88f6b168a20082ced890ef9dc6a322667a8bcf7758848e |
| axos/audit/08_retest.py | file | 6792 | 5905fea5810ebf2d242c2cde32dc4cb209912a2141d05b5cc3b99aadd58e3629 |
| axos/audit/09_authority_audit.py | file | 119213 | af9c3aca6a4328372f1bd93982d297aa56aa985e94a253deab09ae245c6d3834 |
| axos/audit/_killpoints.py | file | 1842 | 1b16e4dc6393e0b6497feff65330b13737166bd858be04adc97f651cec6cb965 |
| axos/audit/_migkill.py | file | 522 | f16ee6df3d696020292ea510a9533b7f73f17ae4a829a124c9c94907b8f472b1 |
| axos/audit/stray_process_audit.py | file | 1675 | 9ec28bbce78dd71e383b5a0d8be3c99b439e7eeb3771c22ca85e9ce0fce8dd48 |
| axos/exec/__init__.py | file | 86 | be5f68abd149694100fbead609427af5b49408e2a89ba8e819f597c908720344 |
| axos/exec/boot.py | file | 31335 | 7dce251f1a58779257a886e49a55a0ccf5db417b55269137bebaf9df91e7ed9c |
| axos/exec/finalizer.py | file | 13101 | f744eaf52a2f55cd9d902c62098cbed87ddb4c328dcd318108baa468a9c89caa |
| axos/exec/identity.py | file | 1784 | 065a65d7533d27f764006fc60019569269e4d0cad5d850afc1ba439b4e933d4d |
| axos/exec/policy.py | file | 51331 | a359f1df5aba2ced1dfae6b64b0aa31de5a2011b883072afebc3c62508a712f2 |
| axos/exec/reconciler.py | file | 28467 | 376e9d8da2d3a5622dc94bbcf1a6d7b432233215bc29a86540534845019095cb |
| axos/exec/recovery.py | file | 60022 | e3164fa52c03f2cefed81d998abb7eba1b1cb06abcceb31e1f482c9af04364d1 |
| axos/exec/resilience.py | file | 36603 | ac51908ce927b7f6d0b15610668a98d33036419dd7dad5d1747e321cf8716766 |
| axos/exec/scheduler.py | file | 23313 | 373b473991dbde10a74d207ace15fe7e3d907dd409b5019f76ba15e556036f28 |
| axos/exec/supervisor.py | file | 59498 | 556c9500ee289a2cf85a6bbcb1a34d67c19aeb8cd89310999fd6f702b8a2e1de |
| axos/exec/synthetic.py | file | 15355 | 1d146d72243e181b28a3a7b6114aa26b5e8efe5ff93df8f380c78648e165f5a6 |
| axos/exec/watchdog.py | file | 24864 | e0a6116a53966f63840b9ff655f86fc5bb7740392cc7f7098130ca8953b92870 |
| axos/exec/worker.py | file | 16874 | b69b2c9b21116e43c88fec059fc90a5c582f91b672ed1ff19e85b23ccfaa8b84 |
| axos/release_manifest.json | file | 6344 | 2fa26d3ef74ee603d69b9d64ca54b7eb7c84f092ee0c283b3138b2672ad911eb |
| axos/store.db | file | 0 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
| axos/store/__init__.py | file | 1000 | 11bfed10907bf964dd5af22183c7e3aca1ae21ddb9522954fe5f1459fd371fa6 |
| axos/store/db.py | file | 13145 | 85204951954816bb4c60d281d20d2a09151d558b8200964949b8893c29c9d6dc |
| axos/store/gate.py | file | 292659 | 7d11086427d1a0e89fccd0dff438e1eba1ec083060a7732a0bd296bcbac11123 |
| axos/store/migrations.py | file | 28723 | d386ab9e2599a7e293d56d86b6e77c4e26d80c1aa77392fb6cb1857f996e7bb5 |
| axos/store/store.db | file | 0 | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
| axos/store/transitions.py | file | 3319 | 312a5c4e511d787b25b8ec8a35f14e65564f0b0b60cdff643e43d0ab184d072a |
| axos/tests/_crasher.py | file | 1995 | df5f0f37204bc6dbfcc1a9905900109e525b7cb537ae657917e1d1bbe7847235 |
| axos/tests/_supdrv.py | file | 5328 | e2233de51f130ab4ce1110f92ded55009c77f44d13dd112d28f9560c892e6a1b |
| axos/tests/r5_helpers.py | file | 3938 | 6a8fff332104d0e5d64e2e8e2783b1d917be07c818e1e9068c2501c71ea2191b |
| axos/tests/test_artifacts_r5.py | file | 50831 | d182d938fb6ad688b3ed1198be5c8b54109ba3849747c1c2a3575582b2c62ede |
| axos/tests/test_boot_r6.py | file | 58734 | d8707950e9e423da0a4b4bc5f54477b1608fb9e052cb2f6ec229bfcd61e69c43 |
| axos/tests/test_exec.py | file | 53128 | 121e0fb9605b779832120f17b189d87417f19a5cf3c3d8412c4a0243087acff6 |
| axos/tests/test_expiry_r4.py | file | 37673 | d38f688e4c50df5e88fa66cdb2439a5981911b215f236dc3069a91a6d268f629 |
| axos/tests/test_fence_enforce.py | file | 26922 | e6371b1aaa7b4c17b8a1f891b50fa1417068a239d86e071a6cd3ef5016cf3e26 |
| axos/tests/test_final_hardening_r14.py | file | 366437 | e68ac802b8557a94cb2f9049d776886063e01ad254909b83f22d230115924ea0 |
| axos/tests/test_finalization_r13.py | file | 136316 | 391420798f273afc40176151381f181bde26659ae70721461f30b5d62d072c5a |
| axos/tests/test_heartbeat_r3.py | file | 36023 | 67d086d2a06013210643cd5d86e77d4ee4aafe6051de8860149f8cd0caf505d4 |
| axos/tests/test_reclaim.py | file | 20610 | 93d0e8f0338775dfdc5b793936ff82afc7c21da0f7f858869d1231423af6c275 |
| axos/tests/test_reconciliation_r11.py | file | 97717 | faee897281da1ed70ae0ad2fe94b9e789cc15b98c2d65f107d5758fc6603683e |
| axos/tests/test_recovery_r8.py | file | 77236 | 67ab4c9b10de283e49e66ee8ead7b83ad29e539b29a97c692800712e11f207a8 |
| axos/tests/test_recovery_r9.py | file | 72933 | 83182ccaaff0a275c193315097c1a4ff9f6cc2b81e9cf8ff83cb36786753d106 |
| axos/tests/test_remediation.py | file | 15496 | 5b1ff4df42666131e7736b7c3e9afa722d806fd43e56457b9a1bde4ffe0fb9a6 |
| axos/tests/test_resilience_r12.py | file | 116899 | 1c0a90e762701d70da4cb394553c26dea351f378cdd33835541f0b9508271a79 |
| axos/tests/test_scheduler_r10.py | file | 78102 | fce6b8fde820bcc1d1035c6149b7a6286be6d3463a2198a6c44df45b348ec53a |
| axos/tests/test_store.py | file | 25631 | 9bb46cbf2f85fb832c8b70ba2bddaf164c85c0e111aa86dbba0055563edb9ae7 |
| axos/tests/test_watchdog_r7.py | file | 58892 | 5bb6c116413ed21e84a46609f96eb48249137ade70504fea81e4544590731abe |