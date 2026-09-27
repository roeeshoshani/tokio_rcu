#![cfg(loom)]

use std::{pin::pin, task::Poll};

use loom::sync::Arc;
use tokio_rcu::rcu_box::RcuBox;

fn busy_block_on_future<F, R>(future: F) -> R
where
    F: Future<Output = R>,
{
    let mut pinned = pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        tokio_rcu::loom_tests_api::on_before_task_poll();

        if let Poll::Ready(res) = pinned.as_mut().poll(&mut context) {
            return res;
        }

        tokio_rcu::loom_tests_api::on_after_task_poll();

        loom::thread::yield_now();
    }
}

/// a type used to detect use after free scenarios.
///
/// this type contains a 64-bit magic value which initially contains one of 256 hardcoded pre-defined random 64-bit integers, chosen according
/// to the chosen id for this uaf detector.
///
/// when this type is dropped, it overwrites the magic with a "freed" magic, which indicates that this type was freed.
///
/// furthermore, if the memory containing this type is freed and is then re-used for another purpose, it will most likely be overwritten with some
/// value that does not match one of the pre-defined valid magic values.
/// this is of course unless it is overwritten with another UAF detector, in which case it will look valid even though it is actually a UAF situation.
/// so, it is advised to pre-allocate all UAF detectors in advance, or just to avoid freeing one and then allocating another.
struct UafDetector {
    magic: u64,
}
impl UafDetector {
    const VALID_MAGICS: [u64; 256] = [
        0xac8a55cd92688677,
        0x95989a3e0b1d326a,
        0x2b3bfed4ae982065,
        0x19b675e67c96c55d,
        0x65fd2cf9514260d5,
        0x6eafc7ab184e56f,
        0x30f7d1bbe69deab9,
        0xaa8ed948d5bd3bdc,
        0xab66f438b27429c5,
        0xe4224e86f6388041,
        0xca41612391f76c7b,
        0x23835a5b10aef63,
        0xb4b5118928a93573,
        0xdf8fb7172e5f497c,
        0x5b6f835450d5c16a,
        0x3a35d20adfb71bdf,
        0x6f03add1bb493828,
        0xb467b0c8247dcc1e,
        0x3b01e217ac8f7c98,
        0x969aac76f223176c,
        0xa055ac4aa47d9527,
        0x82416dea3efb64be,
        0x77b315a410d8030,
        0xcb6476a19d944f87,
        0x351ec83afb890d13,
        0x44a64add64f1377d,
        0x30e67bc9553b71b4,
        0x6a4112c9be200ac6,
        0xab324ce7c9ffb018,
        0x840ebd9bb802c751,
        0xe281264c9e299215,
        0x9f1c0396a16f3e72,
        0xeeae1e7da98d3713,
        0xba0bb382e5ae5622,
        0x7c2e8f59a52fc328,
        0xfe025952e312257b,
        0x936c38f1b6194396,
        0x1582100726939a3a,
        0x930f910b3e694e87,
        0x98707fec36083de2,
        0x8a827ccc042ac1bd,
        0xdcfebc88cb390148,
        0x6076f0a38a44f4e1,
        0xed30162ec2ed0329,
        0x3c36d14c061d8504,
        0xaa44832b5d261242,
        0x44301528f045c14e,
        0x5c52dea31b038b53,
        0xcdad89e8a464db13,
        0x1ffd49ec03bca583,
        0xb1d0f48ff9de5acd,
        0x52fa145226878bda,
        0x6c2d3a39e970397f,
        0x1fab619b1ee88c21,
        0x3a123ab1c1df2479,
        0xfab23d636857e28,
        0xa489cac8c4575e35,
        0xc8baafd77e0e138d,
        0x7ab99d19617de4ec,
        0x2d19932473dd3a44,
        0x96ad1e31b8bfe9c0,
        0xa7c16b39cc091a37,
        0x26ade5e9b09feffa,
        0x3297886e79745f69,
        0xc5d36e8ca87750a8,
        0x7d81dc1e1c36f8c2,
        0x146c7e3f8d24c8ce,
        0x36bb0459a429bef4,
        0xd6f18c3460724ad2,
        0x85306caa6281a824,
        0xd705e7554c6ff319,
        0xe1bfadc10823ca6b,
        0x4a18a75890081943,
        0x4e4637a95314554c,
        0xc3c1f11b6e5ae72b,
        0x9b7266fce6c2fe37,
        0xb43085e78c4a048b,
        0x73e8d7cdb2b56e7f,
        0x8284acde89bcde8,
        0xebe820011837bb96,
        0x48337cc0f68b21be,
        0xb49cf8932da71433,
        0x16fe31c9457ca5ab,
        0xa7b7b74ed45f76a,
        0x2b182b2bda34ff59,
        0x7b825c4d538e3b5c,
        0x3b17a4d86eb7faad,
        0x5aeb1221d9891dd,
        0xa9f9f72e4039a30f,
        0xce6824f71be3902,
        0x7f25b68a69c632f2,
        0x269d999279e7edf6,
        0x6e803addff271d44,
        0xfa27221e4cf1741a,
        0x5a3bd25fc8b6f882,
        0xf7d685051b8a664c,
        0xe7d14ad3ac1bb26b,
        0xfae7fe944782922d,
        0xa6322bd8dd2ecf6d,
        0xaf575d036447b401,
        0xe0599ea4fe0bfdc7,
        0xd5be1a1aa46d9b37,
        0xe2d4c483ee3f27e3,
        0x6e27af8ccb58b669,
        0x68fbcbf47d11a372,
        0x57535858b6b9167c,
        0x5839a637726b7028,
        0xb7a6a4ca75d76b94,
        0xbe074d596ca3e911,
        0xb63337a9ffeb6bc2,
        0x17515a754676fed3,
        0x73ddc8383f2ca631,
        0x9b0287da8f243ca2,
        0x4754197c2dfd0313,
        0xe199e8557f714f9e,
        0xee6b95b5d193c8df,
        0x19533bd93c91c906,
        0x6b560347a712a4a2,
        0xb658f97ccbf3a2c7,
        0xa100c7ab393c5e99,
        0xc1ff5ed841b33b5b,
        0x431f646290428d7d,
        0x9659d943e5d3841d,
        0xd068cd5421ff3e37,
        0xfe7a1e330288c394,
        0x14ad564bd6c209fa,
        0xbe27250a626d261b,
        0xf6cb0efa4c1c5b49,
        0x749d35f4acb6f1a4,
        0xa41dc0191d4df517,
        0x7182c209955d70b6,
        0x87a4d163c265de95,
        0xfea1be4cdcbf65fe,
        0x4639d2482043cdc7,
        0xf04300e5d01253d1,
        0x63fe48903e1090d3,
        0x30d586216fb683e1,
        0x14134321f43e95a,
        0x6f8197f79eb50d2e,
        0x49286ad3fac9cd79,
        0xff3f5b0410f97c0c,
        0xe7fe88f567200a07,
        0x972f32f4c6c90d4e,
        0x7d8c4ef34e94a840,
        0x34acf553dfb4e26a,
        0x30b744be9529cc46,
        0xd44b97ab6638bc03,
        0x13209633cb6c4744,
        0x3ed841b0f7a1ba2e,
        0x217a400fc6a8de19,
        0xfb4296cbcf91082,
        0x6f16ccb96e23c6,
        0xefd3a3981843c4c6,
        0x3956582e7070afa7,
        0xdb1055c166d68cd2,
        0x9de8656e2ba3e5b4,
        0x8c1c840498d58ac2,
        0xfbc8a28fa71eb787,
        0x6a2f21bfc4ba2444,
        0xc66019f76e3543c,
        0x1d801825c3a753b9,
        0xc4612b52c3ecaccf,
        0xa749bf3f5e3c4a94,
        0x4549598ec55a4557,
        0x6817c7266d4cbff5,
        0x94a50961f1c3f5dd,
        0xfefb302387c6ea2f,
        0xdf34e9da4c47bf29,
        0x5d5af6692f4c0430,
        0x5a300955c3328308,
        0x6f4078baee7799c6,
        0xeb881276d05fbc40,
        0xce7bdb6c53cbe475,
        0xc948eea861065311,
        0x2d6b33b6b77f5642,
        0xe26ed9ae825b3474,
        0x6f44e09ac9f40389,
        0xeea3bc4c5089a32f,
        0x2044c9a8daa7ce9b,
        0x9b21967ac6538cc0,
        0x811f2812cc1a8077,
        0x2a8492cdbd5c89cc,
        0x1100c46e7ea1e060,
        0xaf23a0218ddb7d08,
        0x7782c96077b2bbfc,
        0x6717e6cbdf8b6d44,
        0x24836ab60d1b3712,
        0xb556f9aa9453cbdb,
        0xcb3ff94107c51596,
        0xd30a6571c2c4c124,
        0x4ec73cb783b43286,
        0x209ba07add7a034a,
        0x61093280c180b442,
        0x20f43ea465b52774,
        0x2e9e042b0f6dfeb5,
        0x160a473015483489,
        0x8a8fb7ae5dde8bc2,
        0xd9c865ae54c56ce3,
        0x6a50b28c99288735,
        0x79e5bdc50d776bc5,
        0x96ae7931e60f116f,
        0xa9eba7e3357edf6a,
        0xb80e60d29ece4a72,
        0x266500dd951399c6,
        0x7a8c090f4c87bbb6,
        0xb965f26169c685ca,
        0x998fe25dda05ed0,
        0x4740901e61a6cefe,
        0xaa1f8ec62a9d97d,
        0xfe1ebd80b0222ffc,
        0xa25a5b327486197f,
        0x95e90f0c4af66ddc,
        0xd5db71f59c4271d5,
        0xa679b4da6a404803,
        0x66c8e69dd37f950a,
        0xa062ea26c724de0a,
        0xc7c1fea6efbf7132,
        0xe83002ed89a979f0,
        0x82349328f711f300,
        0x337b4b33c1c39beb,
        0xfb98cd16c26ff128,
        0xc9e0ba2d9c6be50d,
        0xd904fe70306e9248,
        0x4cd3b02302487865,
        0x8cfa75d3b8cdb3bc,
        0x31ac2526d48434d0,
        0xd41502eeb06f9c02,
        0xdf359fc05d01f080,
        0x6d898ee8f14cba03,
        0xe9f2fe06981ee6ab,
        0xb696857c33e54628,
        0xa072aae4be70102d,
        0xe16652e31d056c86,
        0x4b6353f95f3ce13d,
        0xccb47d798fe58a56,
        0x994b4edbbecd1628,
        0xcf85b02439d4c0f4,
        0xd5268c133600963f,
        0xd2e977b28fb02ffc,
        0xe55ffd316065d526,
        0xb0da9e41baf03454,
        0xce50c54a4ddec98a,
        0xd7808c82a060a309,
        0xe3b60ccf881176fd,
        0x39f25afbd8ca570f,
        0x95de35019c9f70d9,
        0xe08aa07f7fe5d328,
        0xec01dc4c4c2b7046,
        0x1b469eb4eb99d7a3,
        0xe7d7c18391cd1a51,
        0xdefe57436afe6407,
        0x5801469b73f1c766,
        0xa7d16b0f22d423af,
        0xca20f1699dffea26,
        0x8086ed361c54b945,
        0x2057909077ba016b,
    ];

    const FREED_MAGIC: u64 = 0xb23063e51ef5f2a4;

    /// creates a new UAF detector with the given id.
    pub fn new(id: u8) -> Self {
        Self {
            magic: Self::VALID_MAGICS[id as usize],
        }
    }

    /// returns the id of this UAF detector.
    /// if this UAF detector has already been freed, this function safely detects the UAF and prints a corresponding error message.
    pub fn id(&self) -> u8 {
        Self::VALID_MAGICS
            .iter()
            .position(|x| *x == self.magic)
            .unwrap_or_else(|| {
                panic!("detected use after free, magic = {:#x}", self.magic);
            })
            .try_into()
            .unwrap()
    }
}
impl Drop for UafDetector {
    fn drop(&mut self) {
        self.magic = Self::FREED_MAGIC;
    }
}

#[test]
fn no_uaf_basic() {
    loom::model(|| {
        let uaf_detector_0 = Box::new(UafDetector::new(0));
        let uaf_detector_1 = Box::new(UafDetector::new(1));

        let state = Arc::new(RcuBox::new(uaf_detector_0));
        let worker1 = loom::thread::spawn({
            let state = state.clone();
            move || {
                let prev = busy_block_on_future(state.swap(uaf_detector_1));
                assert_eq!(prev.id(), 0);
                tokio_rcu::loom_tests_api::on_thread_stop();
            }
        });

        // worker 2
        {
            tokio_rcu::loom_tests_api::on_before_task_poll();
            state.with(|guard| {
                let id = guard.id();
                assert!(id == 0 || id == 1);
            });
            tokio_rcu::loom_tests_api::on_after_task_poll();
            tokio_rcu::loom_tests_api::on_thread_stop();
        }

        worker1.join().unwrap();
    })
}
